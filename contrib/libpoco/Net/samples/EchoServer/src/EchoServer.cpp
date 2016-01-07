//
// EchoServer.cpp
//
// $Id: //poco/1.4/Net/samples/EchoServer/src/EchoServer.cpp#1 $
//
// This sample demonstrates the SocketReactor and SocketAcceptor classes.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/SocketReactor.h"
#include "Poco/Net/SocketAcceptor.h"
#include "Poco/Net/SocketNotification.h"
#include "Poco/Net/StreamSocket.h"
#include "Poco/Net/ServerSocket.h"
#include "Poco/NObserver.h"
#include "Poco/Exception.h"
#include "Poco/Thread.h"
#include "Poco/FIFOBuffer.h"
#include "Poco/Delegate.h"
#include "Poco/Util/ServerApplication.h"
#include "Poco/Util/Option.h"
#include "Poco/Util/OptionSet.h"
#include "Poco/Util/HelpFormatter.h"
#include <iostream>


using Poco::Net::SocketReactor;
using Poco::Net::SocketAcceptor;
using Poco::Net::ReadableNotification;
using Poco::Net::WritableNotification;
using Poco::Net::ShutdownNotification;
using Poco::Net::ServerSocket;
using Poco::Net::StreamSocket;
using Poco::NObserver;
using Poco::AutoPtr;
using Poco::Thread;
using Poco::FIFOBuffer;
using Poco::delegate;
using Poco::Util::ServerApplication;
using Poco::Util::Application;
using Poco::Util::Option;
using Poco::Util::OptionSet;
using Poco::Util::HelpFormatter;


class EchoServiceHandler
	/// I/O handler class. This class (un)registers handlers for I/O based on
	/// data availability. To ensure non-blocking behavior and alleviate spurious
	/// socket writability callback triggering when no data to be sent is available,
	/// FIFO buffers are used. I/O FIFOBuffer sends notifications on transitions
	/// from [1] non-readable (i.e. empty) to readable, [2] writable to non-writable 
	/// (i.e. full) and [3] non-writable (i.e. full) to writable.
	/// Based on these notifications, the handler member functions react by
	/// enabling/disabling respective reactor framework notifications.
{
public:
	EchoServiceHandler(StreamSocket& socket, SocketReactor& reactor):
		_socket(socket),
		_reactor(reactor),
		_fifoIn(BUFFER_SIZE, true),
		_fifoOut(BUFFER_SIZE, true)
	{
		Application& app = Application::instance();
		app.logger().information("Connection from " + socket.peerAddress().toString());

		_reactor.addEventHandler(_socket, NObserver<EchoServiceHandler, ReadableNotification>(*this, &EchoServiceHandler::onSocketReadable));
		_reactor.addEventHandler(_socket, NObserver<EchoServiceHandler, ShutdownNotification>(*this, &EchoServiceHandler::onSocketShutdown));

		_fifoOut.readable += delegate(this, &EchoServiceHandler::onFIFOOutReadable);
		_fifoIn.writable += delegate(this, &EchoServiceHandler::onFIFOInWritable);
	}
	
	~EchoServiceHandler()
	{
		Application& app = Application::instance();
		try
		{
			app.logger().information("Disconnecting " + _socket.peerAddress().toString());
		}
		catch (...)
		{
		}
		_reactor.removeEventHandler(_socket, NObserver<EchoServiceHandler, ReadableNotification>(*this, &EchoServiceHandler::onSocketReadable));
		_reactor.removeEventHandler(_socket, NObserver<EchoServiceHandler, WritableNotification>(*this, &EchoServiceHandler::onSocketWritable));
		_reactor.removeEventHandler(_socket, NObserver<EchoServiceHandler, ShutdownNotification>(*this, &EchoServiceHandler::onSocketShutdown));

		_fifoOut.readable -= delegate(this, &EchoServiceHandler::onFIFOOutReadable);
		_fifoIn.writable -= delegate(this, &EchoServiceHandler::onFIFOInWritable);
	}
	
	void onFIFOOutReadable(bool& b)
	{
		if (b)
			_reactor.addEventHandler(_socket, NObserver<EchoServiceHandler, WritableNotification>(*this, &EchoServiceHandler::onSocketWritable));
		else
			_reactor.removeEventHandler(_socket, NObserver<EchoServiceHandler, WritableNotification>(*this, &EchoServiceHandler::onSocketWritable));
	}
	
	void onFIFOInWritable(bool& b)
	{
		if (b)
			_reactor.addEventHandler(_socket, NObserver<EchoServiceHandler, ReadableNotification>(*this, &EchoServiceHandler::onSocketReadable));
		else
			_reactor.removeEventHandler(_socket, NObserver<EchoServiceHandler, ReadableNotification>(*this, &EchoServiceHandler::onSocketReadable));
	}
	
	void onSocketReadable(const AutoPtr<ReadableNotification>& pNf)
	{
		// some socket implementations (windows) report available 
		// bytes on client disconnect, so we  double-check here
		if (_socket.available())
		{
			int len = _socket.receiveBytes(_fifoIn);
			_fifoIn.drain(_fifoOut.write(_fifoIn.buffer(), _fifoIn.used()));
		}
	}
	
	void onSocketWritable(const AutoPtr<WritableNotification>& pNf)
	{
		_socket.sendBytes(_fifoOut);
	}

	void onSocketShutdown(const AutoPtr<ShutdownNotification>& pNf)
	{
		delete this;
	}
	
private:
	enum
	{
		BUFFER_SIZE = 1024
	};
	
	StreamSocket   _socket;
	SocketReactor& _reactor;
	FIFOBuffer     _fifoIn;
	FIFOBuffer     _fifoOut;
};


class EchoServer: public Poco::Util::ServerApplication
	/// The main application class.
	///
	/// This class handles command-line arguments and
	/// configuration files.
	/// Start the EchoServer executable with the help
	/// option (/help on Windows, --help on Unix) for
	/// the available command line options.
	///
	/// To use the sample configuration file (EchoServer.properties),
	/// copy the file to the directory where the EchoServer executable
	/// resides. If you start the debug version of the EchoServer
	/// (EchoServerd[.exe]), you must also create a copy of the configuration
	/// file named EchoServerd.properties. In the configuration file, you
	/// can specify the port on which the server is listening (default
	/// 9977) and the format of the date/time string sent back to the client.
	///
	/// To test the EchoServer you can use any telnet client (telnet localhost 9977).
{
public:
	EchoServer(): _helpRequested(false)
	{
	}
	
	~EchoServer()
	{
	}

protected:
	void initialize(Application& self)
	{
		loadConfiguration(); // load default configuration files, if present
		ServerApplication::initialize(self);
	}
		
	void uninitialize()
	{
		ServerApplication::uninitialize();
	}

	void defineOptions(OptionSet& options)
	{
		ServerApplication::defineOptions(options);
		
		options.addOption(
			Option("help", "h", "display help information on command line arguments")
				.required(false)
				.repeatable(false));
	}

	void handleOption(const std::string& name, const std::string& value)
	{
		ServerApplication::handleOption(name, value);

		if (name == "help")
			_helpRequested = true;
	}

	void displayHelp()
	{
		HelpFormatter helpFormatter(options());
		helpFormatter.setCommand(commandName());
		helpFormatter.setUsage("OPTIONS");
		helpFormatter.setHeader("An echo server implemented using the Reactor and Acceptor patterns.");
		helpFormatter.format(std::cout);
	}

	int main(const std::vector<std::string>& args)
	{
		if (_helpRequested)
		{
			displayHelp();
		}
		else
		{
			// get parameters from configuration file
			unsigned short port = (unsigned short) config().getInt("EchoServer.port", 9977);
			
			// set-up a server socket
			ServerSocket svs(port);
			// set-up a SocketReactor...
			SocketReactor reactor;
			// ... and a SocketAcceptor
			SocketAcceptor<EchoServiceHandler> acceptor(svs, reactor);
			// run the reactor in its own thread so that we can wait for 
			// a termination request
			Thread thread;
			thread.start(reactor);
			// wait for CTRL-C or kill
			waitForTerminationRequest();
			// Stop the SocketReactor
			reactor.stop();
			thread.join();
		}
		return Application::EXIT_OK;
	}
	
private:
	bool _helpRequested;
};


int main(int argc, char** argv)
{
	EchoServer app;
	return app.run(argc, argv);
}
