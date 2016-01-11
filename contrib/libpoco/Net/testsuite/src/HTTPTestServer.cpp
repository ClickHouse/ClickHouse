//
// HTTPTestServer.cpp
//
// $Id: //poco/1.4/Net/testsuite/src/HTTPTestServer.cpp#4 $
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "HTTPTestServer.h"
#include "Poco/Net/StreamSocket.h"
#include "Poco/Net/SocketAddress.h"
#include "Poco/Timespan.h"
#include "Poco/NumberFormatter.h"
#include <iostream>


using Poco::Net::Socket;
using Poco::Net::StreamSocket;
using Poco::Net::SocketAddress;
using Poco::NumberFormatter;


const std::string HTTPTestServer::SMALL_BODY("This is some random text data returned by the server");
const std::string HTTPTestServer::LARGE_BODY(4000, 'x');


HTTPTestServer::HTTPTestServer():
	_socket(SocketAddress()),
	_thread("HTTPTestServer"),
	_stop(false)
{
	_thread.start(*this);
	_ready.wait();
	_lastRequest.reserve(4000);
}


HTTPTestServer::~HTTPTestServer()
{
	_stop = true;
	_thread.join();
}


Poco::UInt16 HTTPTestServer::port() const
{
	return _socket.address().port();
}


const std::string& HTTPTestServer::lastRequest() const
{
	return _lastRequest;
}


void HTTPTestServer::run()
{
	_ready.set();
	Poco::Timespan span(250000);
	while (!_stop)
	{
		if (_socket.poll(span, Socket::SELECT_READ))
		{
			StreamSocket ss = _socket.acceptConnection();
			try
			{
				_lastRequest.clear();
				char buffer[256];
				int n = ss.receiveBytes(buffer, sizeof(buffer));
				while (n > 0 && !_stop)
				{
					_lastRequest.append(buffer, n);
					if (!requestComplete())
						n = ss.receiveBytes(buffer, sizeof(buffer));
					else
						n = 0;
				}
				std::string response = handleRequest();
				ss.sendBytes(response.data(), (int) response.size());
				Poco::Thread::sleep(1000);
				try
				{
					ss.shutdown();
					Poco::Thread::sleep(1000);
				}
				catch (Poco::Exception&)
				{
				}
			}
			catch (Poco::Exception& exc)
			{
				std::cerr << "HTTPTestServer: " << exc.displayText() << std::endl;
			}
		}
	}
}


bool HTTPTestServer::requestComplete() const
{
	return ((_lastRequest.substr(0, 3) == "GET" || _lastRequest.substr(0, 4) == "HEAD") && 
	        (_lastRequest.find("\r\n\r\n") != std::string::npos)) ||
	        (_lastRequest.find("\r\n0\r\n") != std::string::npos);
}


std::string HTTPTestServer::handleRequest() const
{
	std::string response;
	response.reserve(16000);
	if (_lastRequest.substr(0, 10) == "GET /small" ||
	    _lastRequest.substr(0, 11) == "HEAD /small")
	{
		std::string body(SMALL_BODY);
		response.append("HTTP/1.0 200 OK\r\n");
		response.append("Content-Type: text/plain\r\n");
		response.append("Content-Length: "); 
		response.append(NumberFormatter::format((int) body.size()));
		response.append("\r\n");
		response.append("Connection: Close\r\n");
		response.append("\r\n");
		if (_lastRequest.substr(0, 3) == "GET")
			response.append(body);
	}
	else if (_lastRequest.substr(0, 10) == "GET /large" ||
	         _lastRequest.substr(0, 11) == "HEAD /large" ||
	         _lastRequest.substr(0, 36) == "GET http://www.somehost.com:80/large")
	{
		std::string body(LARGE_BODY);
		response.append("HTTP/1.0 200 OK\r\n");
		response.append("Content-Type: text/plain\r\n");
		response.append("Content-Length: "); 
		response.append(NumberFormatter::format((int) body.size()));
		response.append("\r\n");
		response.append("Connection: Close\r\n");
		response.append("\r\n");
		if (_lastRequest.substr(0, 3) == "GET")
			response.append(body);
	}
	else if (_lastRequest.substr(0, 4) == "POST")
	{
		std::string::size_type pos = _lastRequest.find("\r\n\r\n");
		pos += 4;
		std::string body = _lastRequest.substr(pos);
		response.append("HTTP/1.0 200 OK\r\n");
		response.append("Content-Type: text/plain\r\n");
		if (_lastRequest.find("Content-Length") != std::string::npos)
		{
			response.append("Content-Length: "); 
			response.append(NumberFormatter::format((int) body.size()));
			response.append("\r\n");
		}
		else if (_lastRequest.find("chunked") != std::string::npos)
		{
			response.append("Transfer-Encoding: chunked\r\n");
		}
		response.append("Connection: Close\r\n");
		response.append("\r\n");
		response.append(body);
	}
	else if (_lastRequest.substr(0, 15) == "HEAD /keepAlive")
	{
		std::string body(SMALL_BODY);
		response.append("HTTP/1.1 200 OK\r\n");
		response.append("Connection: keep-alive\r\n");
		response.append("Content-Type: text/plain\r\n");
		response.append("Content-Length: "); 
		response.append(NumberFormatter::format((int) body.size()));
		response.append("\r\n\r\n");
		response.append("HTTP/1.1 200 OK\r\n");
		response.append("Connection: Keep-Alive\r\n");
		response.append("Content-Type: text/plain\r\n");
		response.append("Content-Length: "); 
		response.append(NumberFormatter::format((int) body.size()));
		response.append("\r\n\r\n");
		response.append(body);
		body = LARGE_BODY;
		response.append("HTTP/1.1 200 OK\r\n");
		response.append("Connection: keep-alive\r\n");
		response.append("Content-Type: text/plain\r\n");
		response.append("Transfer-Encoding: chunked\r\n\r\n");
		response.append(NumberFormatter::formatHex((unsigned) body.length()));
		response.append("\r\n");
		response.append(body);
		response.append("\r\n0\r\n\r\n");
		response.append("HTTP/1.1 200 OK\r\n");
		response.append("Connection: close\r\n");
		response.append("Content-Type: text/plain\r\n");
		response.append("Content-Length: "); 
		response.append(NumberFormatter::format((int) body.size()));
		response.append("\r\n\r\n");
	}
	else if (_lastRequest.substr(0, 13) == "GET /redirect")
	{
		response.append("HTTP/1.0 302 Found\r\n");
		response.append("Location: /large\r\n");
		response.append("\r\n");
	}
	else if (_lastRequest.substr(0, 13) == "GET /notfound")
	{
		response.append("HTTP/1.0 404 Not Found\r\n");
		response.append("\r\n");
	}
	else if (_lastRequest.substr(0, 5) == "GET /" ||
	    _lastRequest.substr(0, 6) == "HEAD /")
	{
		std::string body(SMALL_BODY);
		response.append("HTTP/1.0 200 OK\r\n");
		response.append("Content-Type: text/plain\r\n");
		response.append("Content-Length: "); 
		response.append(NumberFormatter::format((int) body.size()));
		response.append("\r\n");
		response.append("Connection: Close\r\n");
		response.append("\r\n");
		if (_lastRequest.substr(0, 3) == "GET")
			response.append(body);
	}
	return response;
}
