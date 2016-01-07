//
// Connection.h
//
// $Id$
//
// Library: MongoDB
// Package: MongoDB
// Module:  Connection
//
// Definition of the Connection class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MongoDB_Connection_INCLUDED
#define MongoDB_Connection_INCLUDED


#include "Poco/Net/SocketAddress.h"
#include "Poco/Net/StreamSocket.h"
#include "Poco/Mutex.h"
#include "Poco/MongoDB/RequestMessage.h"
#include "Poco/MongoDB/ResponseMessage.h"


namespace Poco {
namespace MongoDB {


class MongoDB_API Connection
	/// Represents a connection to a MongoDB server
{
public:
	typedef Poco::SharedPtr<Connection> Ptr;

	Connection();
		/// Default constructor. Use this when you want to
		/// connect later on.

	Connection(const std::string& hostAndPort);
		/// Constructor which connects to the given MongoDB host/port.
		/// The host and port must be separated with a colon.

	Connection(const std::string& host, int port);
		/// Constructor which connects to the given MongoDB host/port.

	Connection(const Net::SocketAddress& addrs);
		/// Constructor which connects to the given MongoDB host/port.

	virtual ~Connection();
		/// Destructor

	Net::SocketAddress address() const;
		/// Returns the address of the MongoDB connection

	void connect(const std::string& hostAndPort);
		/// Connects to the given MongoDB server. The host and port must be separated
		/// with a colon.

	void connect(const std::string& host, int port);
		/// Connects to the given MongoDB server.

	void connect(const Net::SocketAddress& addrs);
		/// Connects to the given MongoDB server.

	void disconnect();
		/// Disconnects from the MongoDB server

	void sendRequest(RequestMessage& request);
		/// Sends a request to the MongoDB server
		/// Only use this when the request hasn't a response.

	void sendRequest(RequestMessage& request, ResponseMessage& response);
		/// Sends a request to the MongoDB server and receives the response.
		/// Use this when a response is expected: only a query or getmore
		/// request will return a response.

private:
	Net::SocketAddress _address;
	Net::StreamSocket _socket;
	void connect();
		/// Connects to the MongoDB server
};


inline Net::SocketAddress Connection::address() const
{
	return _address;
}


} } // namespace Poco::MongoDB


#endif //MongoDB_Connection_INCLUDED
