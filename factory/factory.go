package main

import (
	"flag"
	"fmt"
	"net"
	"net/rpc"
	"uk.ac.bris.cs/distributed3/pairbroker/stubs"
)

type Factory struct{}

var stack []int

//TODO: Define a Multiply function to be accessed via RPC.
//Check the previous weeks' examples to figure out how to do this.

func (f *Factory) Multiply(pair stubs.Pair, jr *stubs.JobReport) (err error) {
	fmt.Println(pair.X, " * ", pair.Y, " = ", pair.X*pair.Y)
	jr.Result = pair.X * pair.Y
	stack = append(stack, pair.X*pair.Y)
	return
}

func (f *Factory) Divide(pair stubs.Pair, jr *stubs.JobReport) (err error) {
	fmt.Println(pair.X, " / ", pair.Y, " = ", pair.X/pair.Y)
	jr.Result = pair.X / pair.Y
	return
}
func check2Divide(client *rpc.Client) {
	for {

		if len(stack) >= 2 {
			newpair := stubs.Pair{X: stack[0], Y: stack[1]}

			//Form a request to publish it in 'multiply'
			towork := stubs.PublishRequest{Topic: "divide", Pair: newpair}
			status := new(stubs.StatusReport)
			_ = client.Go(stubs.Publish, towork, status, nil)
			stack = stack[2:]
		}
	}
}
func main() {
	pAddr := flag.String("ip", "127.0.0.1:8050", "IP and port to listen on")
	brokerAddr := flag.String("broker", "127.0.0.1:8030", "Address of broker instance")
	//topic := flag.String("topic","multiply", "Topic this factory will publish to")
	flag.Parse()
	//TODO: You'll need to set up the RPC server, and subscribe to the running broker instance.
	_ = rpc.Register(&Factory{})
	client, _ := rpc.Dial("tcp", *brokerAddr)
	defer client.Close()
	status := new(stubs.StatusReport)
	client.Call(stubs.CreateChannel, stubs.ChannelRequest{Topic: "divide", Buffer: 10}, status)
	reqd := stubs.Subscription{Topic: "divide", FactoryAddress: *pAddr, Callback: "Factory.Divide"}
	resd := new(stubs.StatusReport)
	client.Go(stubs.Subscribe, reqd, resd, nil)
	req := stubs.Subscription{Topic: "multiply", FactoryAddress: *pAddr, Callback: "Factory.Multiply"}
	res := new(stubs.StatusReport)
	client.Go(stubs.Subscribe, req, res, nil)

	listener, _ := net.Listen("tcp", *pAddr)
	defer listener.Close()

	go check2Divide(client)
	rpc.Accept(listener)
}
