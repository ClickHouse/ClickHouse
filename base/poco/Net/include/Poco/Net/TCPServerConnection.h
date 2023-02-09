//
// TCPServerConnection.h
//
// Library: Net
// Package: TCPServer
// Module:  TCPServerConnection
//
// Definition of the TCPServerConnection class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_TCPServerConnection_INCLUDED
#define Net_TCPServerConnection_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/StreamSocket.h"
#include "Poco/Runnable.h"


namespace Poco {
namespace Net {


class Net_API TCPServerConnection: public Poco::Runnable
	/// The abstract base class for TCP server connections
	/// created by TCPServer.
	///
	/// Derived classes must override the run() method
	/// (inherited from Runnable). Furthermore, a
	/// TCPServerConnectionFactory must be provided for the subclass.
	///
	/// The run() method must perform the complete handling
	/// of the client connection. As soon as the run() method
	/// returns, the server connection object is destroyed and
	/// the connection is automatically closed.
	///
	/// A new TCPServerConnection object will be created for
	/// each new client connection that is accepted by
	/// TCPServer.
{
public:
	TCPServerConnection(const StreamSocket& socket);
		/// Creates the TCPServerConnection using the given
		/// stream socket.

	virtual ~TCPServerConnection();
		/// Destroys the TCPServerConnection.

protected:
	StreamSocket& socket();
		/// Returns a reference to the underlying socket.

	void start();
		/// Calls run() and catches any exceptions that
		/// might be thrown by run().

private:
	TCPServerConnection();
	TCPServerConnection(const TCPServerConnection&);
	TCPServerConnection& operator = (const TCPServerConnection&);
	
	StreamSocket _socket;
	
	friend class TCPServerDispatcher;
};


//
// inlines
//
inline StreamSocket& TCPServerConnection::socket()
{
	return _socket;
}


} } // namespace Poco::Net


#endif // Net_TCPServerConnection_INCLUDED
