//
// SecureServerSocketImpl.h
//
// Library: NetSSL_Win
// Package: SSLSockets
// Module:  SecureServerSocketImpl
//
// Definition of the SecureServerSocketImpl class.
//
// Copyright (c) 2006-2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NetSSL_SecureServerSocketImpl_INCLUDED
#define NetSSL_SecureServerSocketImpl_INCLUDED


#include "Poco/Net/NetSSL.h"
#include "Poco/Net/SecureSocketImpl.h"
#include "Poco/Net/ServerSocketImpl.h"
#include "Poco/Net/Context.h"


namespace Poco {
namespace Net {


class NetSSL_Win_API SecureServerSocketImpl: public ServerSocketImpl
	/// The SocketImpl class for SecureServerSocket.
{
public:
	SecureServerSocketImpl(Context::Ptr pContext);
		/// Creates the SecureServerSocketImpl using the
		/// given SSL context object.

	SocketImpl* acceptConnection(SocketAddress& clientAddr);
		/// Get the next completed connection from the
		/// socket's completed connection queue.
		///
		/// If the queue is empty, waits until a connection
		/// request completes.
		///
		/// Returns a new TCP socket for the connection
		/// with the client.
		///
		/// The client socket's address is returned in clientAddr.
	
	void connect(const SocketAddress& address);
		/// Not supported by this kind of socket.
		///
		/// Throws a Poco::InvalidAccessException.

	void connect(const SocketAddress& address, const Poco::Timespan& timeout);
		/// Not supported by this kind of socket.
		///
		/// Throws a Poco::InvalidAccessException.

	void connectNB(const SocketAddress& address);
		/// Not supported by this kind of socket.
		///
		/// Throws a Poco::InvalidAccessException.
	
	void bind(const SocketAddress& address, bool reuseAddress = false);
		/// Bind a local address to the socket.
		///
		/// This is usually only done when establishing a server
		/// socket. TCP clients should not bind a socket to a
		/// specific address.
		///
		/// If reuseAddress is true, sets the SO_REUSEADDR
		/// socket option.
		
	void listen(int backlog = 64);
		/// Puts the socket into listening state.
		///
		/// The socket becomes a passive socket that
		/// can accept incoming connection requests.
		///
		/// The backlog argument specifies the maximum
		/// number of connections that can be queued
		/// for this socket.

	void close();
		/// Close the socket.
	
	int sendBytes(const void* buffer, int length, int flags = 0);
		/// Not supported by this kind of socket.
		///
		/// Throws a Poco::InvalidAccessException.
	
	int receiveBytes(void* buffer, int length, int flags = 0);
		/// Not supported by this kind of socket.
		///
		/// Throws a Poco::InvalidAccessException.
	
	int sendTo(const void* buffer, int length, const SocketAddress& address, int flags = 0);
		/// Not supported by this kind of socket.
		///
		/// Throws a Poco::InvalidAccessException.
	
	int receiveFrom(void* buffer, int length, SocketAddress& address, int flags = 0);
		/// Not supported by this kind of socket.
		///
		/// Throws a Poco::InvalidAccessException.
	
	void sendUrgent(unsigned char data);
		/// Not supported by this kind of socket.
		///
		/// Throws a Poco::InvalidAccessException.

	bool secure() const;
		/// Returns true iff the socket's connection is secure
		/// (using SSL or TLS).
		
	Context::Ptr context() const;
		/// Returns the SSL context used by this socket.

protected:
	~SecureServerSocketImpl();
		/// Destroys the SecureServerSocketImpl.

private:
	SecureServerSocketImpl(const SecureServerSocketImpl&);
	SecureServerSocketImpl& operator = (const SecureServerSocketImpl&);

private:
	SecureSocketImpl _impl;
};


//
// inlines
//
inline Context::Ptr SecureServerSocketImpl::context() const
{
	return _impl.context();
}


} } // namespace Poco::Net


#endif // NetSSL_SecureServerSocketImpl_INCLUDED
