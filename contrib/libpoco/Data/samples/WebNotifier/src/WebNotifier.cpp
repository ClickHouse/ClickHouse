//
// WebNotifier.cpp
//
// $Id: //poco/Main/Data/samples/WebNotifier/src/WebNotifier.cpp#2 $
//
// This sample demonstrates a combination of Data and Net libraries by
// creating a database, registering callbacks for insert/update events 
// and sending database modifications to the web client through web socket.
// Since callbacks are only registered for session, in order to see the
// effects, database updates should be done through the shell provided by
// this example. 
// 
// This is only a demo. For production-grade a better web socket management
// facility as well as persisting notification functionality (e.g. via 
// triggers and external functions) should be used.
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0


#include "Poco/Delegate.h"
#include "Poco/Timespan.h"
#include "Poco/Exception.h"
#include "Poco/Net/HTTPServer.h"
#include "Poco/Net/HTTPRequestHandler.h"
#include "Poco/Net/HTTPRequestHandlerFactory.h"
#include "Poco/Net/HTTPServerParams.h"
#include "Poco/Net/HTTPServerRequest.h"
#include "Poco/Net/HTTPServerResponse.h"
#include "Poco/Net/HTTPServerParams.h"
#include "Poco/Net/ServerSocket.h"
#include "Poco/Net/WebSocket.h"
#include "Poco/Net/NetException.h"
#include "Poco/Data/Session.h"
#include "Poco/Data/RowFormatter.h"
#include "Poco/Data/RecordSet.h"
#include "Poco/Data/SQLite/Notifier.h"
#include <iostream>

using Poco::delegate;
using Poco::Timespan;
using Poco::Exception;
using Poco::NullPointerException;

using Poco::Net::ServerSocket;
using Poco::Net::WebSocket;
using Poco::Net::WebSocketException;
using Poco::Net::HTTPRequestHandler;
using Poco::Net::HTTPRequestHandlerFactory;
using Poco::Net::HTTPServer;
using Poco::Net::HTTPServerRequest;
using Poco::Net::HTTPResponse;
using Poco::Net::HTTPServerResponse;
using Poco::Net::HTTPServerParams;

using namespace Poco::Data::Keywords;
using Poco::Data::Session;
using Poco::Data::Statement;
using Poco::Data::RowFormatter;
using Poco::Data::RecordSet;
using Poco::Data::SQLite::Notifier;


#define PROMPT "sql>"


class PageRequestHandler: public HTTPRequestHandler
	/// Return a HTML document with some JavaScript creating
	/// a WebSocket connection.
{
public:
	void handleRequest(HTTPServerRequest& request, HTTPServerResponse& response)
	{
		response.setChunkedTransferEncoding(true);
		response.sendFile("WebNotifier.html", "text/html");
	}
};


/////////////////
//  WebSocket  //
/////////////////


class WebSocketRequestHandler: public HTTPRequestHandler
	/// Handler for the WebSocket connection.
{
public:
	WebSocketRequestHandler() : _pWS(0), _flags(0)
	{
	}

	~WebSocketRequestHandler()
	{
		shutdown();
	}

	void shutdown()
	{
		if (_pWS)
		{
			_pWS->shutdown();
			delete _pWS;
		}
	}

	void send(const std::string& buffer)
		/// Pushes data to web client.
	{
		std::cout << "Sending data: " << buffer << std::endl;
		_pWS->sendFrame(buffer.data(), (int) buffer.size(), _flags);
	}

	void handleRequest(HTTPServerRequest& request, HTTPServerResponse& response)
		/// Creates WebSocket and accepts the connection request from web client.
	{
		try
		{
			if (!_pWS)
			{
				_pWS = new WebSocket(request, response);
				Timespan ts(600, 0);
				_pWS->setReceiveTimeout(ts);
				_pWS->setSendTimeout(ts);
			}
			std::cout << std::endl << "WebSocket connection established." << std::endl << PROMPT;

			char buffer[1024];
			int n, count = 0;
			do
			{
				n = _pWS->receiveFrame(buffer, sizeof(buffer), _flags);
			}
			while (n > 0 || (_flags & WebSocket::FRAME_OP_BITMASK) != WebSocket::FRAME_OP_CLOSE);
				std::cout << "WebSocket connection closed." << std::endl;
		}
		catch (WebSocketException& exc)
		{
			std::cout << exc.displayText() << std::endl;
			switch (exc.code())
			{
			case WebSocket::WS_ERR_HANDSHAKE_UNSUPPORTED_VERSION:
				response.set("Sec-WebSocket-Version", WebSocket::WEBSOCKET_VERSION);
				// fallthrough
			case WebSocket::WS_ERR_NO_HANDSHAKE:
			case WebSocket::WS_ERR_HANDSHAKE_NO_VERSION:
			case WebSocket::WS_ERR_HANDSHAKE_NO_KEY:
				response.setStatusAndReason(HTTPResponse::HTTP_BAD_REQUEST);
				response.setContentLength(0);
				response.send();
				break;
			}
		}
	}

public:
	WebSocket* _pWS;
	int        _flags;
};


class RequestHandlerFactory: public HTTPRequestHandlerFactory
	/// Web request handler factory.
{
public:
	RequestHandlerFactory() : _pHandler(0)
	{
	}

	HTTPRequestHandler* createRequestHandler(const HTTPServerRequest& request)
	{
		std::string uri = request.getURI();
		if (uri == "/")
		{
			return new PageRequestHandler;
		}
		else if (uri == "/ws")
		{
			if (!_pHandler) _pHandler = new WebSocketRequestHandler;
			return _pHandler;
		}

		if (uri != "/favicon.ico")
			std::cout << "Unknown URI: " << uri << std::endl;
		
		return 0;
	}

	WebSocketRequestHandler& handler()
	{
		if (!_pHandler) throw NullPointerException("WebSocket not connected.");
		return *_pHandler;
	}

private:
	WebSocketRequestHandler* _pHandler;
};


////////////////
//  Database  //
////////////////


class CSVFormatter : public RowFormatter
	/// Formatter, passed to DB statement.
{
public:

	std::string& formatValues(const ValueVec& vals, std::string& formattedValues)
		/// Formats the result into comma separated list of values.
	{
		std::ostringstream str;

		ValueVec::const_iterator it = vals.begin();
		ValueVec::const_iterator end = vals.end();
		for (; it != end;)
		{
			str << it->convert<std::string>();
			if (++it != end) str << ',';
			else break;
		}

		return formattedValues = str.str();
	}
};


class DBEventHandler
	/// Handler for DB insert/update events.
{
public:
	DBEventHandler(RequestHandlerFactory& factory):
		_session("SQLite", "sample.db"),
		_factory(factory),
		_notifier(_session)
		/// Constructor; opens/initializes the database and associates
		/// notification events with their respective handlers.
	{
		initDB();
		_notifier.insert += delegate(this, &DBEventHandler::onInsert);
		_notifier.update += delegate(this, &DBEventHandler::onUpdate);
	}

	~DBEventHandler()
		/// Destructor; unregisters the notification events.
	{
		_notifier.insert -= delegate(this, &DBEventHandler::onInsert);
		_notifier.update -= delegate(this, &DBEventHandler::onUpdate);
	}
	
	std::size_t execute(const std::string& sql)
		/// Exectutes the SQL statement.
	{
		Statement stmt = (_session << sql);
		return stmt.execute();
	}

	Session& session()
	{
		return _session;
	}

private:
	void initDB()
	{
		_session << "DROP TABLE IF EXISTS Person", now;
		_session << "CREATE TABLE Person (Name VARCHAR(30), Address VARCHAR, Age INTEGER(3))", now;
	}

	Notifier* notifier(const void* pSender)
	{
		return reinterpret_cast<Notifier*>(const_cast<void*>(pSender));
	}

	void notify(Poco::Int64 rowID)
		/// Executes the query and sends the data to the web client.
	{
		std::ostringstream os;
		CSVFormatter cf;
		Statement stmt = (_session << "SELECT rowid, Name, Address, Age FROM Person WHERE rowid = ?", use(rowID), format(cf), now);
		os << RecordSet(stmt);
		_factory.handler().send(os.str());
	}

	void onInsert(const void* pSender)
		/// Insert event handler; retrieves the data for the affected row
		/// and calls notify.
	{
		Notifier* pN = notifier(pSender);
		Poco::Int64 rowID = pN->getRow();
		std::cout << "Inserted row " << rowID << std::endl;
		notify(rowID);
	}

	void onUpdate(const void* pSender)
		/// Update event handler; retrieves the data for the affected row
		/// and calls notify.
	{
		Notifier* pN = notifier(pSender);
		Poco::Int64 rowID = pN->getRow();
		std::cout << "Updated row " << rowID << std::endl;
		notify(rowID);
	}

	Session                _session;
	RequestHandlerFactory& _factory;
	Notifier               _notifier;
};


void doHelp()
	/// Displays help.
{
	std::cout << "Poco Data/Net example - HTML Page notifications from DB events" << std::endl;
	std::cout << "" << std::endl;
	std::cout << "To observe the functionality, take following steps:" << std::endl;
	std::cout << "" << std::endl;
	std::cout << "1) Run a web browser and connect to http://localhost:9980 ." << std::endl;
	std::cout << "2) Wait until \"WebSocket connection established.\" is displayed." << std::endl;
	std::cout << "3) Issue SQL commands to see the web page updated, e.g.:" << std::endl;
	std::cout << "\tINSERT INTO Person VALUES('Homer Simpson', 'Springfield', 42);" << std::endl;
	std::cout << "\tINSERT INTO Person VALUES('bart Simpson', 'Springfield', 12);" << std::endl;
	std::cout << "\tUPDATE Person SET Age=38 WHERE Name='Homer Simpson';" << std::endl;
	std::cout << "\tUPDATE Person SET Name='Bart Simpson' WHERE Name='bart Simpson';" << std::endl;
	std::cout << "" << std::endl;
	std::cout << "To end the program, enter \"exit\"." << std::endl;
	std::cout << "" << std::endl;
	std::cout << "To view this help, enter \"help\" or \"?\"." << std::endl;
}


void doShell(DBEventHandler& dbEventHandler)
	/// Displays the shell and dispatches commands.
{
	doHelp();

	while (true)
	{
		std::cout << PROMPT;
		char cmd[512] = {0};
		std::cin.getline(cmd, 512);
		if (strncmp(cmd, "exit", 4) == 0)
			break;
		try
		{
			if ((strncmp(cmd, "help", 4) == 0) || cmd[0] == '?')
				doHelp();
			if (strlen(cmd) > 0)
			{
				std::size_t rows = dbEventHandler.execute(cmd);
				std::cout << rows << " row" << ((rows != 1) ? "s" : "") << " affected." << std::endl;
			}
		}
		catch(Exception& ex)
		{
			std::cout << ex.displayText() << std::endl;
		}
	}
}


///////////
//  Main //
///////////

int main(int argc, char** argv)
{
	// HTTPServer instance
	RequestHandlerFactory* pFactory = new RequestHandlerFactory;
	HTTPServer srv(pFactory, 9980);

	// DB stuff
	DBEventHandler dbEventHandler(*pFactory);

	// Start the HTTPServer
	srv.start();

	// Run shell
	doShell(dbEventHandler);

	// Stop the HTTPServer
	srv.stop();

	return 0;
}
