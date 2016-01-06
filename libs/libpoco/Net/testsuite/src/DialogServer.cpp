//
// DialogServer.cpp
//
// $Id: //poco/1.4/Net/testsuite/src/DialogServer.cpp#1 $
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DialogServer.h"
#include "Poco/Net/DialogSocket.h"
#include "Poco/Net/SocketAddress.h"
#include "Poco/Timespan.h"
#include <iostream>


using Poco::Net::Socket;
using Poco::Net::DialogSocket;
using Poco::Net::SocketAddress;
using Poco::FastMutex;
using Poco::Thread;


DialogServer::DialogServer(bool acceptCommands):
	_socket(SocketAddress()),
	_thread("DialogServer"),
	_stop(false),
	_acceptCommands(acceptCommands),
	_log(false)
{
	_thread.start(*this);
	_ready.wait();
}


DialogServer::~DialogServer()
{
	_stop = true;
	_thread.join();
}


Poco::UInt16 DialogServer::port() const
{
	return _socket.address().port();
}


void DialogServer::run()
{
	_ready.set();
	Poco::Timespan span(250000);
	while (!_stop)
	{
		if (_socket.poll(span, Socket::SELECT_READ))
		{
			DialogSocket ds = _socket.acceptConnection();
			{
				FastMutex::ScopedLock lock(_mutex);
				if (!_nextResponses.empty())
				{
					ds.sendMessage(_nextResponses.front());
					_nextResponses.erase(_nextResponses.begin());
				}
			}
			if (_acceptCommands)
			{
				try
				{
					std::string command;
					while (ds.receiveMessage(command))
					{
						if (_log) std::cout << ">> " << command << std::endl;
						{
							FastMutex::ScopedLock lock(_mutex);
							_lastCommands.push_back(command);
							if (!_nextResponses.empty())
							{
								if (_log) std::cout << "<< " << _nextResponses.front() << std::endl;
								ds.sendMessage(_nextResponses.front());
								_nextResponses.erase(_nextResponses.begin());
							}
						}
					}
				}
				catch (Poco::Exception& exc)
				{
					std::cerr << "DialogServer: " << exc.displayText() << std::endl;
				}
			}
		}
	}
}


const std::string& DialogServer::lastCommand() const
{
	FastMutex::ScopedLock lock(_mutex);
	
	static const std::string EMPTY;
	if (_lastCommands.empty())
		return EMPTY;
	else
		return _lastCommands.back();
}


const std::vector<std::string>& DialogServer::lastCommands() const
{
	return _lastCommands;
}


std::string DialogServer::popCommand()
{
	FastMutex::ScopedLock lock(_mutex);

	std::string command;
	if (!_lastCommands.empty())
	{
		command = _lastCommands.front();
		_lastCommands.erase(_lastCommands.begin());
	}
	return command;
}


std::string DialogServer::popCommandWait()
{
	std::string result(popCommand());
	while (result.empty())
	{
		Thread::sleep(100);
		result = popCommand();
	}
	return result;
}


void DialogServer::addResponse(const std::string& response)
{
	FastMutex::ScopedLock lock(_mutex);

	_nextResponses.push_back(response);
}

	
void DialogServer::clearCommands()
{
	FastMutex::ScopedLock lock(_mutex);

	_lastCommands.clear();
}

		
void DialogServer::clearResponses()
{
	FastMutex::ScopedLock lock(_mutex);
	
	_nextResponses.clear();
}


void DialogServer::log(bool flag)
{
	_log = flag;
}
