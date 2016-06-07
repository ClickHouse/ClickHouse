//
// DialogServer.h
//
// $Id: //poco/1.4/Net/testsuite/src/DialogServer.h#1 $
//
// Definition of the DialogServer class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DialogServer_INCLUDED
#define DialogServer_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/ServerSocket.h"
#include "Poco/Net/StreamSocket.h"
#include "Poco/Thread.h"
#include "Poco/Event.h"
#include "Poco/Mutex.h"
#include <vector>


class DialogServer: public Poco::Runnable
	/// A server for testing FTPClientSession and friends.
{
public:
	DialogServer(bool acceptCommands = true);
		/// Creates the DialogServer.

	~DialogServer();
		/// Destroys the DialogServer.

	Poco::UInt16 port() const;
		/// Returns the port the echo server is
		/// listening on.
		
	void run();
		/// Does the work.
		
	const std::string& lastCommand() const;
		/// Returns the last command received by the server.

	std::string popCommand();
		/// Pops the next command from the list of received commands.

	std::string popCommandWait();
		/// Pops the next command from the list of received commands.
		/// Waits until a command is available.

	const std::vector<std::string>& lastCommands() const;
		/// Returns the last command received by the server.
		
	void addResponse(const std::string& response);
		/// Sets the next response returned by the server.
	
	void clearCommands();
		/// Clears all commands.
		
	void clearResponses();
		/// Clears all responses.
		
	void log(bool flag);
		/// Enables or disables logging to stdout.
	
private:
	Poco::Net::ServerSocket  _socket;
	Poco::Thread             _thread;
	Poco::Event              _ready;
	mutable Poco::FastMutex  _mutex;
	bool                     _stop;
	std::vector<std::string> _nextResponses;
	std::vector<std::string> _lastCommands;
	bool                     _acceptCommands;
	bool                     _log;
};


#endif // DialogServer_INCLUDED
