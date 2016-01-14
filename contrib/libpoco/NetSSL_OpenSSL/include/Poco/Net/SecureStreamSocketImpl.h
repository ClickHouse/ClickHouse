//
// SecureStreamSocketImpl.h
//
// $Id: //poco/1.4/NetSSL_OpenSSL/include/Poco/Net/SecureStreamSocketImpl.h#2 $
//
// Library: NetSSL_OpenSSL
// Package: SSLSockets
// Module:  SecureStreamSocketImpl
//
// Definition of the SecureStreamSocketImpl class.
//
// Copyright (c) 2006-2010, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NetSSL_SecureStreamSocketImpl_INCLUDED
#define NetSSL_SecureStreamSocketImpl_INCLUDED


#include "Poco/Net/NetSSL.h"
#include "Poco/Net/SecureSocketImpl.h"
#include "Poco/Net/StreamSocketImpl.h"
#include "Poco/Net/Context.h"
#include "Poco/Net/X509Certificate.h"


namespace Poco {
namespace Net {


class NetSSL_API SecureStreamSocketImpl: public StreamSocketImpl
	/// This class implements a SSL stream socket.
{
public:
	SecureStreamSocketImpl(Context::Ptr pContext);
		/// Creates the SecureStreamSocketImpl.

	SecureStreamSocketImpl(StreamSocketImpl* pStreamSocket, Context::Ptr pContext);
		/// Creates the SecureStreamSocketImpl.

	SocketImpl* acceptConnection(SocketAddress& clientAddr);
		/// Not supported by a SecureStreamSocket.
		///
		/// Throws a Poco::InvalidAccessException.

	void connect(const SocketAddress& address);
		/// Initializes the socket and establishes a connection to 
		/// the TCP server at the given address.
		///
		/// Can also be used for UDP sockets. In this case, no
		/// connection is established. Instead, incoming and outgoing
		/// packets are restricted to the specified address.

	void connect(const SocketAddress& address, const Poco::Timespan& timeout);
		/// Initializes the socket, sets the socket timeout and 
		/// establishes a connection to the TCP server at the given address.

	void connectNB(const SocketAddress& address);
		/// Initializes the socket and establishes a connection to 
		/// the TCP server at the given address. Prior to opening the
		/// connection the socket is set to nonblocking mode.
		
	void bind(const SocketAddress& address, bool reuseAddress = false);
		/// Not supported by a SecureStreamSocket.
		///
		/// Throws a Poco::InvalidAccessException.
		
	void listen(int backlog = 64);
		/// Not supported by a SecureStreamSocket.
		///
		/// Throws a Poco::InvalidAccessException.

	void close();
		/// Close the socket.
	
	int sendBytes(const void* buffer, int length, int flags = 0);
		/// Sends the contents of the given buffer through
		/// the socket. Any specified flags are ignored.
		///
		/// Returns the number of bytes sent, which may be
		/// less than the number of bytes specified.
	
	int receiveBytes(void* buffer, int length, int flags = 0);
		/// Receives data from the socket and stores it
		/// in buffer. Up to length bytes are received.
		///
		/// Returns the number of bytes received.
	
	int sendTo(const void* buffer, int length, const SocketAddress& address, int flags = 0);
		/// Not supported by a SecureStreamSocket.
		///
		/// Throws a Poco::InvalidAccessException.
	
	int receiveFrom(void* buffer, int length, SocketAddress& address, int flags = 0);
		/// Not supported by a SecureStreamSocket.
		///
		/// Throws a Poco::InvalidAccessException.
	
	void sendUrgent(unsigned char data);
		/// Not supported by a SecureStreamSocket.
		///
		/// Throws a Poco::InvalidAccessException.

	int available();
		/// Returns the number of bytes available that can be read
		/// without causing the socket to block.
		///
		/// For an SSL connection, returns the number of bytes that
		/// can be read from the currently buffered SSL record,
		/// before a new record is read from the underlying socket.

	void shutdownReceive();
		/// Shuts down the receiving part of the socket connection.
		///
		/// Since SSL does not support a half shutdown, this does
		/// nothing.
		
	void shutdownSend();
		/// Shuts down the receiving part of the socket connection.
		///
		/// Since SSL does not support a half shutdown, this does
		/// nothing.
		
	void shutdown();
		/// Shuts down the SSL connection.
		
	void abort();
		/// Aborts the connection by closing the underlying
		/// TCP connection. No orderly SSL shutdown is performed.
		
	bool secure() const;
		/// Returns true iff the socket's connection is secure
		/// (using SSL or TLS).

	void setPeerHostName(const std::string& hostName);
		/// Sets the peer host name for certificate validation purposes.
		
	const std::string& getPeerHostName() const;
		/// Returns the peer host name.

	bool havePeerCertificate() const;
		/// Returns true iff the peer has presented a
		/// certificate.

	X509Certificate peerCertificate() const;
		/// Returns the peer's X509 certificate.
		///
		/// Throws a SSLException if the peer did not
		/// present a certificate.
		
	Context::Ptr context() const;
		/// Returns the SSL context used by this socket.

	void setLazyHandshake(bool flag = true);
		/// Enable lazy SSL handshake. If enabled, the SSL handshake
		/// will be performed the first time date is sent or
		/// received over the connection.
		
	bool getLazyHandshake() const;
		/// Returns true if setLazyHandshake(true) has been called.

	void verifyPeerCertificate();
		/// Performs post-connect (or post-accept) peer certificate validation,
		/// using the peer's IP address as host name.

	void verifyPeerCertificate(const std::string& hostName);
		/// Performs post-connect (or post-accept) peer certificate validation
		/// using the given host name.

	int completeHandshake();
		/// Completes the SSL handshake.
		///
		/// If the SSL connection was the result of an accept(),
		/// the server-side handshake is completed, otherwise
		/// a client-side handshake is performed. 

	Session::Ptr currentSession();
		/// Returns the SSL session of the current connection,
		/// for reuse in a future connection (if session caching
		/// is enabled).
		///
		/// If no connection is established, returns null.
		
	void useSession(Session::Ptr pSession);
		/// Sets the SSL session to use for the next
		/// connection. Setting a previously saved Session
		/// object is necessary to enable session caching.
		///
		/// To remove the currently set session, a null pointer
		/// can be given.
		///
		/// Must be called before connect() to be effective.
		
	bool sessionWasReused();
		/// Returns true iff a reused session was negotiated during
		/// the handshake.
		
protected:
	void acceptSSL();
		/// Performs a SSL server-side handshake.
	
	void connectSSL();
		/// Performs a SSL client-side handshake on an already connected TCP socket.
	
	~SecureStreamSocketImpl();
		/// Destroys the SecureStreamSocketImpl.

	static int lastError();
	static void error();
	static void error(const std::string& arg);
	static void error(int code);
	static void error(int code, const std::string& arg);

private:
	SecureStreamSocketImpl(const SecureStreamSocketImpl&);
	SecureStreamSocketImpl& operator = (const SecureStreamSocketImpl&);

	SecureSocketImpl _impl;
	bool             _lazyHandshake;

	friend class SecureSocketImpl;
	friend class SecureStreamSocket;
};


//
// inlines
//
inline const std::string& SecureStreamSocketImpl::getPeerHostName() const
{
	return _impl.getPeerHostName();
}


inline void SecureStreamSocketImpl::setPeerHostName(const std::string& peerHostName)
{
	_impl.setPeerHostName(peerHostName);
}


inline Context::Ptr SecureStreamSocketImpl::context() const
{
	return _impl.context();
}


inline Session::Ptr SecureStreamSocketImpl::currentSession()
{
	return _impl.currentSession();
}

	
inline void SecureStreamSocketImpl::useSession(Session::Ptr pSession)
{
	_impl.useSession(pSession);
}

	
inline bool SecureStreamSocketImpl::sessionWasReused()
{
	return _impl.sessionWasReused();
}


inline int SecureStreamSocketImpl::lastError()
{
	return SocketImpl::lastError();
}


inline void SecureStreamSocketImpl::error()
{
	return SocketImpl::error();
}


inline void SecureStreamSocketImpl::error(const std::string& arg)
{
	return SocketImpl::error(arg);
}


inline void SecureStreamSocketImpl::error(int code)
{
	return SocketImpl::error(code);
}


inline void SecureStreamSocketImpl::error(int code, const std::string& arg)
{
	return SocketImpl::error(code, arg);
}


} } // namespace Poco::Net


#endif // NetSSL_SecureStreamSocketImpl_INCLUDED
