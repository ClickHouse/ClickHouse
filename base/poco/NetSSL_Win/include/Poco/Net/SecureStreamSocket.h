//
// SecureStreamSocket.h
//
// Library: NetSSL_Win
// Package: SSLSockets
// Module:  SecureStreamSocket
//
// Definition of the SecureStreamSocket class.
//
// Copyright (c) 2006-2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NetSSL_SecureStreamSocket_INCLUDED
#define NetSSL_SecureStreamSocket_INCLUDED


#include "Poco/Net/NetSSL.h"
#include "Poco/Net/StreamSocket.h"
#include "Poco/Net/Context.h"
#include "Poco/Net/Session.h"
#include "Poco/Net/X509Certificate.h"


namespace Poco {
namespace Net {


class NetSSL_Win_API SecureStreamSocket: public StreamSocket
	/// A subclass of StreamSocket for secure SSL sockets.
	///
	/// A few notes about nonblocking IO:
	/// sendBytes() and receiveBytes() can return a
	/// negative value when using a nonblocking socket, which means 
	/// a SSL handshake is currently in progress and more data
	/// needs to be read or written for the handshake to continue.
	/// If sendBytes() or receiveBytes() return ERR_SSL_WANT_WRITE,
	/// sendBytes() must be called as soon as possible (usually, after
	/// select() indicates that data can be written). Likewise, if
	/// ERR_SSL_WANT_READ is returned, receiveBytes() must be called
	/// as soon as data is available for reading (indicated by select()).
	///
	/// The SSL handshake is delayed until the first sendBytes() or 
	/// receiveBytes() operation is performed on the socket. No automatic
	/// post connection check (checking the peer certificate for a valid
	/// hostname) is performed when using nonblocking I/O. To manually
	/// perform peer certificate validation, call verifyPeerCertificate()
	/// after the SSL handshake has been completed.
{
public:
	enum
	{
		ERR_SSL_WANT_READ  = -1,
		ERR_SSL_WANT_WRITE = -2
	};
	
	SecureStreamSocket();
		/// Creates an unconnected secure stream socket
		/// using the default client SSL context.
		///
		/// Before sending or receiving data, the socket
		/// must be connected with a call to connect().

	explicit SecureStreamSocket(Context::Ptr pContext);
		/// Creates an unconnected secure stream socket
		/// using the given SSL context.
		///
		/// Before sending or receiving data, the socket
		/// must be connected with a call to connect().

	SecureStreamSocket(Context::Ptr pContext, Session::Ptr pSession);
		/// Creates an unconnected secure stream socket
		/// using the given SSL context.
		///
		/// Before sending or receiving data, the socket
		/// must be connected with a call to connect().
		///
		/// The given Session is reused, if possible (client session
		/// caching is enabled for the given Context, and the server
		/// agrees to reuse the session).

	explicit SecureStreamSocket(const SocketAddress& address);
		/// Creates a secure stream socket using the default 
		/// client SSL context and connects it to
		/// the socket specified by address.

	SecureStreamSocket(const SocketAddress& address, Context::Ptr pContext);
		/// Creates a secure stream socket using the given 
		/// client SSL context and connects it to
		/// the socket specified by address.

	SecureStreamSocket(const SocketAddress& address, Context::Ptr pContext, Session::Ptr pSession);
		/// Creates a secure stream socket using the given 
		/// client SSL context and connects it to
		/// the socket specified by address.
		///
		/// The given Session is reused, if possible (client session
		/// caching is enabled for the given Context, and the server
		/// agrees to reuse the session).

	SecureStreamSocket(const SocketAddress& address, const std::string& hostName);
		/// Creates a secure stream socket using the default 
		/// client SSL context and connects it to
		/// the socket specified by address.
		///
		/// The given host name is used for certificate verification.

	SecureStreamSocket(const SocketAddress& address, const std::string& hostName, Context::Ptr pContext);
		/// Creates a secure stream socket using the given 
		/// client SSL context and connects it to
		/// the socket specified by address.
		///
		/// The given host name is used for certificate verification.

	SecureStreamSocket(const SocketAddress& address, const std::string& hostName, Context::Ptr pContext, Session::Ptr pSession);
		/// Creates a secure stream socket using the given 
		/// client SSL context and connects it to
		/// the socket specified by address.
		///
		/// The given host name is used for certificate verification.
		///
		/// The given Session is reused, if possible (client session
		/// caching is enabled for the given Context, and the server
		/// agrees to reuse the session).

	SecureStreamSocket(const Socket& socket);
		/// Creates the SecureStreamSocket with the SocketImpl
		/// from another socket. The SocketImpl must be
		/// a SecureStreamSocketImpl, otherwise an InvalidArgumentException
		/// will be thrown.

	virtual ~SecureStreamSocket();
		/// Destroys the StreamSocket.

	SecureStreamSocket& operator = (const Socket& socket);
		/// Assignment operator.
		///
		/// Releases the socket's SocketImpl and
		/// attaches the SocketImpl from the other socket and
		/// increments the reference count of the SocketImpl.	
	
	bool havePeerCertificate() const;
		/// Returns true iff the peer has presented a
		/// certificate.

	X509Certificate peerCertificate() const;
		/// Returns the peer's X509 certificate.
		///
		/// Throws a SSLException if the peer did not
		/// present a certificate.
		
	void setPeerHostName(const std::string& hostName);
		/// Sets the peer's host name used for certificate validation.
		
	const std::string& getPeerHostName() const;
		/// Returns the peer's host name used for certificate validation.

	static SecureStreamSocket attach(const StreamSocket& streamSocket);
		/// Creates a SecureStreamSocket over an existing socket
		/// connection. The given StreamSocket must be connected.
		/// A SSL handshake will be performed.

	static SecureStreamSocket attach(const StreamSocket& streamSocket, Context::Ptr pContext);
		/// Creates a SecureStreamSocket over an existing socket
		/// connection. The given StreamSocket must be connected.
		/// A SSL handshake will be performed.

	static SecureStreamSocket attach(const StreamSocket& streamSocket, Context::Ptr pContext, Session::Ptr pSession);
		/// Creates a SecureStreamSocket over an existing socket
		/// connection. The given StreamSocket must be connected.
		/// A SSL handshake will be performed.
		///
		/// The given Session is reused, if possible (client session
		/// caching is enabled for the given Context, and the server
		/// agrees to reuse the session).

	static SecureStreamSocket attach(const StreamSocket& streamSocket, const std::string& peerHostName);
		/// Creates a SecureStreamSocket over an existing socket
		/// connection. The given StreamSocket must be connected.
		/// A SSL handshake will be performed.

	static SecureStreamSocket attach(const StreamSocket& streamSocket, const std::string& peerHostName, Context::Ptr pContext);
		/// Creates a SecureStreamSocket over an existing socket
		/// connection. The given StreamSocket must be connected.
		/// A SSL handshake will be performed.

	static SecureStreamSocket attach(const StreamSocket& streamSocket, const std::string& peerHostName, Context::Ptr pContext, Session::Ptr pSession);
		/// Creates a SecureStreamSocket over an existing socket
		/// connection. The given StreamSocket must be connected.
		/// A SSL handshake will be performed.
		///
		/// The given Session is reused, if possible (client session
		/// caching is enabled for the given Context, and the server
		/// agrees to reuse the session).

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
		/// using the peer host name set with setPeerHostName(), or the peer's
		/// IP address string if no peer host name has been set.
		///
		/// Should only be used for non-blocking connections, after the
		/// initial SSL handshake has been performed (see completeHandshake()).

	void verifyPeerCertificate(const std::string& hostName);
		/// Performs post-connect (or post-accept) peer certificate validation
		/// using the given host name.
		///
		/// Should only be used for non-blocking connections, after the
		/// initial SSL handshake has been performed (see completeHandshake()).
		
	int completeHandshake();
		/// Completes the SSL handshake.
		///
		/// If the SSL connection was the result of an accept(),
		/// the server-side handshake is completed, otherwise
		/// a client-side handshake is performed.
		///
		/// Returns 1 if the handshake was successful, ERR_SSL_WANT_READ or
		/// ERR_SSL_WANT_WRITE if more data is required to complete the
		/// handshake. In this case, completeHandshake() should be called
		/// again, after the necessary condition has been met.

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
		
	void abort();
		/// Aborts the SSL connection by closing the underlying
		/// TCP connection. No orderly SSL shutdown is performed.
		
protected:
	SecureStreamSocket(SocketImpl* pImpl);

	friend class SecureServerSocket;
};


} } // namespace Poco::Net


#endif // NetSSL_SecureStreamSocket_INCLUDED
