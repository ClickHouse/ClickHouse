//
// SecureServerSocket.h
//
// Library: NetSSL_OpenSSL
// Package: SSLSockets
// Module:  SecureServerSocket
//
// Definition of the SecureServerSocket class.
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NetSSL_SecureServerSocket_INCLUDED
#define NetSSL_SecureServerSocket_INCLUDED


#include "Poco/Net/NetSSL.h"
#include "Poco/Net/ServerSocket.h"
#include "Poco/Net/Context.h"


namespace Poco {
namespace Net {


class NetSSL_API SecureServerSocket: public ServerSocket
	/// A server socket for secure SSL connections.
{
public:
	SecureServerSocket();
		/// Creates a SSL server socket using the
		/// default SSL server context.
		///
		/// The server socket must be bound to
		/// an address and put into listening state.

	explicit SecureServerSocket(Context::Ptr pContext);
		/// Creates a SSL server socket, using the
		/// given SSL context object.
		///
		/// The server socket must be bound to
		/// an address and put into listening state.

	SecureServerSocket(const Socket& socket);
		/// Creates the SecureServerSocket with the SocketImpl
		/// from another socket. The SocketImpl must be
		/// a SecureServerSocketImpl, otherwise an InvalidArgumentException
		/// will be thrown.

	SecureServerSocket(const SocketAddress& address, int backlog = 64);
		/// Creates a server socket using the default server SSL context,
		/// binds it to the given address and puts it in listening
		/// state.
		///
		/// After successful construction, the server socket
		/// is ready to accept connections.

	SecureServerSocket(const SocketAddress& address, int backlog, Context::Ptr pContext);
		/// Creates a server socket using the given SSL context, binds it
		/// to the given address and puts it in listening
		/// state.
		///
		/// After successful construction, the server socket
		/// is ready to accept connections.

	SecureServerSocket(Poco::UInt16 port, int backlog = 64);
		/// Creates a server socket using the default server SSL context,
		/// binds it to the given port and puts it in listening
		/// state.
		///
		/// After successful construction, the server socket
		/// is ready to accept connections.

	SecureServerSocket(Poco::UInt16 port, int backlog, Context::Ptr pContext);
		/// Creates a server socket using the given SSL context, binds it
		/// to the given port and puts it in listening
		/// state.
		///
		/// After successful construction, the server socket
		/// is ready to accept connections.

	virtual ~SecureServerSocket();
		/// Destroys the StreamSocket.

	SecureServerSocket& operator = (const Socket& socket);
		/// Assignment operator.
		///
		/// Releases the socket's SocketImpl and
		/// attaches the SocketImpl from the other socket and
		/// increments the reference count of the SocketImpl.	

	StreamSocket acceptConnection(SocketAddress& clientAddr);
		/// Get the next completed connection from the
		/// socket's completed connection queue.
		///
		/// If the queue is empty, waits until a connection
		/// request completes.
		///
		/// Returns a new SSL socket for the connection
		/// with the client.
		///
		/// The client socket's address is returned in clientAddr.
		///
		/// No SSL handshake is performed on the new connection.
		/// The SSL handshake will be performed the first time
		/// sendBytes(), receiveBytes() or completeHandshake()
		/// is called on the returned SecureStreamSocket.

	StreamSocket acceptConnection();
		/// Get the next completed connection from the
		/// socket's completed connection queue.
		///
		/// If the queue is empty, waits until a connection
		/// request completes.
		///
		/// Returns a new SSL socket for the connection
		/// with the client.
		///
		/// No SSL handshake is performed on the new connection.
		/// The SSL handshake will be performed the first time
		/// sendBytes(), receiveBytes() or completeHandshake()
		/// is called on the returned SecureStreamSocket.

	Context::Ptr context() const;
		/// Returns the SSL context used by this socket.
};


} } // namespace Poco::Net


#endif // NetSSL_SecureServerSocket_INCLUDED
