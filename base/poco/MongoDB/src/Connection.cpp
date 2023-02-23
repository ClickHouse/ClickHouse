//
// Connection.cpp
//
// Library: MongoDB
// Package: MongoDB
// Module:  Connection
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/SocketStream.h"
#include "Poco/MongoDB/Connection.h"
#include "Poco/MongoDB/Database.h"
#include "Poco/URI.h"
#include "Poco/Format.h"
#include "Poco/NumberParser.h"
#include "Poco/Exception.h"


namespace Poco {
namespace MongoDB {


Connection::SocketFactory::SocketFactory()
{
}


Connection::SocketFactory::~SocketFactory()
{
}


Poco::Net::StreamSocket Connection::SocketFactory::createSocket(const std::string& host, int port, Poco::Timespan connectTimeout, bool secure)
{
	if (!secure)
	{
		Poco::Net::SocketAddress addr(host, port);
		Poco::Net::StreamSocket socket;
		if (connectTimeout > 0)
			socket.connect(addr, connectTimeout);
		else
			socket.connect(addr);
		return socket;
	}
	else throw Poco::NotImplementedException("Default SocketFactory implementation does not support SecureStreamSocket");
}


Connection::Connection():
	_address(),
	_socket()
{
}


Connection::Connection(const std::string& hostAndPort):
	_address(hostAndPort),
	_socket()
{
	connect();
}


Connection::Connection(const std::string& uri, SocketFactory& socketFactory):
	_address(),
	_socket()
{
	connect(uri, socketFactory);
}


Connection::Connection(const std::string& host, int port):
	_address(host, port),
	_socket()
{
	connect();
}


Connection::Connection(const Poco::Net::SocketAddress& addrs):
	_address(addrs),
	_socket()
{
	connect();
}


Connection::Connection(const Poco::Net::StreamSocket& socket):
	_address(socket.peerAddress()),
	_socket(socket)
{
}


Connection::~Connection()
{
	try
	{
		disconnect();
	}
	catch (...)
	{
	}
}


void Connection::connect()
{
	_socket.connect(_address);
}


void Connection::connect(const std::string& hostAndPort)
{
	_address = Poco::Net::SocketAddress(hostAndPort);
	connect();
}


void Connection::connect(const std::string& host, int port)
{
	_address = Poco::Net::SocketAddress(host, port);
	connect();
}


void Connection::connect(const Poco::Net::SocketAddress& addrs)
{
	_address = addrs;
	connect();
}


void Connection::connect(const Poco::Net::StreamSocket& socket)
{
	_address = socket.peerAddress();
	_socket = socket;
}


void Connection::connect(const std::string& uri, SocketFactory& socketFactory)
{
	Poco::URI theURI(uri);
	if (theURI.getScheme() != "mongodb") throw Poco::UnknownURISchemeException(uri);

	std::string userInfo = theURI.getUserInfo();
	std::string host = theURI.getHost();
	Poco::UInt16 port = theURI.getPort();
	if (port == 0) port = 27017;

	std::string databaseName = theURI.getPath();
	if (!databaseName.empty() && databaseName[0] == '/') databaseName.erase(0, 1);
	if (databaseName.empty()) databaseName = "admin";

	bool ssl = false;
	Poco::Timespan connectTimeout;
	Poco::Timespan socketTimeout;
	std::string authMechanism = Database::AUTH_SCRAM_SHA1;

	Poco::URI::QueryParameters params = theURI.getQueryParameters();
	for (Poco::URI::QueryParameters::const_iterator it = params.begin(); it != params.end(); ++it)
	{
		if (it->first == "ssl")
		{
			ssl = (it->second == "true");
		}
		else if (it->first == "connectTimeoutMS")
		{
			connectTimeout = static_cast<Poco::Timespan::TimeDiff>(1000)*Poco::NumberParser::parse(it->second);
		}
		else if (it->first == "socketTimeoutMS")
		{
			socketTimeout = static_cast<Poco::Timespan::TimeDiff>(1000)*Poco::NumberParser::parse(it->second);
		}
		else if (it->first == "authMechanism")
		{
			authMechanism = it->second;
		}
	}

	connect(socketFactory.createSocket(host, port, connectTimeout, ssl));

	if (socketTimeout > 0)
	{
		_socket.setSendTimeout(socketTimeout);
		_socket.setReceiveTimeout(socketTimeout);
	}

	if (!userInfo.empty())
	{
		std::string username;
		std::string password;
		std::string::size_type pos = userInfo.find(':');
		if (pos != std::string::npos)
		{
			username.assign(userInfo, 0, pos++);
			password.assign(userInfo, pos, userInfo.size() - pos);
		}
		else username = userInfo;

		Database database(databaseName);
		if (!database.authenticate(*this, username, password, authMechanism))
			throw Poco::NoPermissionException(Poco::format("Access to MongoDB database %s denied for user %s", databaseName, username));
	}
}


void Connection::disconnect()
{
	_socket.close();
}


void Connection::sendRequest(RequestMessage& request)
{
	Poco::Net::SocketOutputStream sos(_socket);
	request.send(sos);
}


void Connection::sendRequest(RequestMessage& request, ResponseMessage& response)
{
	sendRequest(request);

	Poco::Net::SocketInputStream sis(_socket);
	response.read(sis);
}


} } // Poco::MongoDB
