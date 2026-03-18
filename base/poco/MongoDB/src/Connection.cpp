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
    std::vector<std::string> strAddresses;
    std::string newURI;

    if (uri.find(',') != std::string::npos)
    {
        size_t pos;
        size_t head = 0;
        if ((pos = uri.find("@")) != std::string::npos)
        {
            head = pos + 1;
        }
        else if ((pos = uri.find("://")) != std::string::npos)
        {
            head = pos + 3;
        }

        std::string tempstr;
        std::string::const_iterator it = uri.begin();
        it += head;
        size_t tail = head;
        for (;it != uri.end() && *it != '?' && *it != '/'; ++it)
        {
            tempstr += *it;
            tail++;
        }

        it = tempstr.begin();
        std::string token;
        for (;it != tempstr.end(); ++it)
        {
            if (*it == ',')
            {
                newURI = uri.substr(0, head) + token + uri.substr(tail, uri.length());
                strAddresses.push_back(newURI);
                token = "";
            }
            else
            {
                token += *it;
            }
        }
        newURI = uri.substr(0, head) + token + uri.substr(tail, uri.length());
        strAddresses.push_back(newURI);
    }
    else
    {
        strAddresses.push_back(uri);
    }

    newURI = strAddresses.front();
    Poco::URI theURI(newURI);
    if (theURI.getScheme() != "mongodb") throw Poco::UnknownURISchemeException(uri);

    std::string userInfo = theURI.getUserInfo();
    std::string databaseName = theURI.getPath();
    if (!databaseName.empty() && databaseName[0] == '/') databaseName.erase(0, 1);
    if (databaseName.empty()) databaseName = "admin";

    bool ssl = false;
    Poco::Timespan connectTimeout;
    Poco::Timespan socketTimeout;
    std::string authMechanism = Database::AUTH_SCRAM_SHA1;
    std::string readPreference="primary";

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
        else if (it->first == "readPreference")
        {
            readPreference= it->second;
        }
    }

    for (std::vector<std::string>::const_iterator it = strAddresses.cbegin();it != strAddresses.cend(); ++it)
    {
        newURI = *it;
        theURI = Poco::URI(newURI);

        std::string host = theURI.getHost();
        Poco::UInt16 port = theURI.getPort();
        if (port == 0) port = 27017;

        connect(socketFactory.createSocket(host, port, connectTimeout, ssl));
        _uri = newURI;
        if (socketTimeout > 0)
        {
            _socket.setSendTimeout(socketTimeout);
            _socket.setReceiveTimeout(socketTimeout);
        }
        if (strAddresses.size() > 1)
        {
            Poco::MongoDB::QueryRequest request("admin.$cmd");
            request.setNumberToReturn(1);
            request.selector().add("isMaster", 1);
            Poco::MongoDB::ResponseMessage response;

            sendRequest(request, response);
            _uri = newURI;
            if (!response.documents().empty())
            {
                Poco::MongoDB::Document::Ptr doc = response.documents()[0];
                if (doc->get<bool>("ismaster") && readPreference == "primary")
                {
                    break;
                }
                else if (!doc->get<bool>("ismaster") && readPreference == "secondary")
                {
                    break;
                }
                else if (it + 1 == strAddresses.cend())
                {
                    throw Poco::URISyntaxException(uri);
                }
            }
        }
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


void Connection::sendRequest(OpMsgMessage& request, OpMsgMessage& response)
{
	Poco::Net::SocketOutputStream sos(_socket);
	request.send(sos);

	response.clear();
	readResponse(response);
}


void Connection::sendRequest(OpMsgMessage& request)
{
	request.setAcknowledgedRequest(false);
	Poco::Net::SocketOutputStream sos(_socket);
	request.send(sos);
}


void Connection::readResponse(OpMsgMessage& response)
{
	Poco::Net::SocketInputStream sis(_socket);
	response.read(sis);
}



} } // Poco::MongoDB
