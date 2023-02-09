//
// SocketImpl.h
//
// Library: Net
// Package: Sockets
// Module:  SocketImpl
//
// Definition of the SocketImpl class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_SocketImpl_INCLUDED
#define Net_SocketImpl_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/SocketDefs.h"
#include "Poco/Net/SocketAddress.h"
#include "Poco/RefCountedObject.h"
#include "Poco/Timespan.h"


namespace Poco {
namespace Net {


class Net_API SocketImpl: public Poco::RefCountedObject
	/// This class encapsulates the Berkeley sockets API.
	///
	/// Subclasses implement specific socket types like
	/// stream or datagram sockets.
	///
	/// You should not create any instances of this class.
{
public:
	enum SelectMode
	{
		SELECT_READ  = 1,
		SELECT_WRITE = 2,
		SELECT_ERROR = 4
	};

	virtual SocketImpl* acceptConnection(SocketAddress& clientAddr);
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

	virtual void connect(const SocketAddress& address);
		/// Initializes the socket and establishes a connection to
		/// the TCP server at the given address.
		///
		/// Can also be used for UDP sockets. In this case, no
		/// connection is established. Instead, incoming and outgoing
		/// packets are restricted to the specified address.

	virtual void connect(const SocketAddress& address, const Poco::Timespan& timeout);
		/// Initializes the socket, sets the socket timeout and
		/// establishes a connection to the TCP server at the given address.

	virtual void connectNB(const SocketAddress& address);
		/// Initializes the socket and establishes a connection to
		/// the TCP server at the given address. Prior to opening the
		/// connection the socket is set to nonblocking mode.

	virtual void bind(const SocketAddress& address, bool reuseAddress = false);
		/// Bind a local address to the socket.
		///
		/// This is usually only done when establishing a server
		/// socket. TCP clients should not bind a socket to a
		/// specific address.
		///
		/// If reuseAddress is true, sets the SO_REUSEADDR
		/// socket option.

	virtual void bind(const SocketAddress& address, bool reuseAddress, bool reusePort );
		/// Bind a local address to the socket.
		///
		/// This is usually only done when establishing a server
		/// socket. TCP clients should not bind a socket to a
		/// specific address.
		///
		/// If reuseAddress is true, sets the SO_REUSEADDR
		/// socket option.
		///
		/// If reusePort is true, sets the SO_REUSEPORT
		/// socket option.

	virtual void bind6(const SocketAddress& address, bool reuseAddress = false, bool ipV6Only = false);
		/// Bind a local IPv6 address to the socket.
		///
		/// This is usually only done when establishing a server
		/// socket. TCP clients should not bind a socket to a
		/// specific address.
		///
		/// If reuseAddress is true, sets the SO_REUSEADDR
		/// socket option.
		///
		/// The given address must be an IPv6 address. The
		/// IPPROTO_IPV6/IPV6_V6ONLY option is set on the socket
		/// according to the ipV6Only parameter.
		///
		/// If the library has not been built with IPv6 support,
		/// a Poco::NotImplementedException will be thrown.

	virtual void bind6(const SocketAddress& address, bool reuseAddress, bool reusePort,  bool ipV6Only);
		/// Bind a local IPv6 address to the socket.
		///
		/// This is usually only done when establishing a server
		/// socket. TCP clients should not bind a socket to a
		/// specific address.
		///
		/// If reuseAddress is true, sets the SO_REUSEADDR
		/// socket option.
		///
		/// If reusePort is true, sets the SO_REUSEPORT
		/// socket option.
		///
		/// The given address must be an IPv6 address. The
		/// IPPROTO_IPV6/IPV6_V6ONLY option is set on the socket
		/// according to the ipV6Only parameter.
		///
		/// If the library has not been built with IPv6 support,
		/// a Poco::NotImplementedException will be thrown.

	virtual void listen(int backlog = 64);
		/// Puts the socket into listening state.
		///
		/// The socket becomes a passive socket that
		/// can accept incoming connection requests.
		///
		/// The backlog argument specifies the maximum
		/// number of connections that can be queued
		/// for this socket.

	virtual void close();
		/// Close the socket.

	virtual void shutdownReceive();
		/// Shuts down the receiving part of the socket connection.

	virtual void shutdownSend();
		/// Shuts down the sending part of the socket connection.

	virtual void shutdown();
		/// Shuts down both the receiving and the sending part
		/// of the socket connection.

	virtual int sendBytes(const void* buffer, int length, int flags = 0);
		/// Sends the contents of the given buffer through
		/// the socket.
		///
		/// Returns the number of bytes sent, which may be
		/// less than the number of bytes specified.
		///
		/// Certain socket implementations may also return a negative
		/// value denoting a certain condition.

	virtual int receiveBytes(void* buffer, int length, int flags = 0);
		/// Receives data from the socket and stores it
		/// in buffer. Up to length bytes are received.
		///
		/// Returns the number of bytes received.
		///
		/// Certain socket implementations may also return a negative
		/// value denoting a certain condition.

	virtual int sendTo(const void* buffer, int length, const SocketAddress& address, int flags = 0);
		/// Sends the contents of the given buffer through
		/// the socket to the given address.
		///
		/// Returns the number of bytes sent, which may be
		/// less than the number of bytes specified.

	virtual int receiveFrom(void* buffer, int length, SocketAddress& address, int flags = 0);
		/// Receives data from the socket and stores it
		/// in buffer. Up to length bytes are received.
		/// Stores the address of the sender in address.
		///
		/// Returns the number of bytes received.

	virtual void sendUrgent(unsigned char data);
		/// Sends one byte of urgent data through
		/// the socket.
		///
		/// The data is sent with the MSG_OOB flag.
		///
		/// The preferred way for a socket to receive urgent data
		/// is by enabling the SO_OOBINLINE option.

	virtual int available();
		/// Returns the number of bytes available that can be read
		/// without causing the socket to block.

	virtual bool poll(const Poco::Timespan& timeout, int mode);
		/// Determines the status of the socket, using a
		/// call to select().
		///
		/// The mode argument is constructed by combining the values
		/// of the SelectMode enumeration.
		///
		/// Returns true if the next operation corresponding to
		/// mode will not block, false otherwise.

	bool pollImpl(Poco::Timespan& timeout, int mode);
		/// Modifies `timeout`

	virtual void setSendBufferSize(int size);
		/// Sets the size of the send buffer.

	virtual int getSendBufferSize();
		/// Returns the size of the send buffer.
		///
		/// The returned value may be different than the
		/// value previously set with setSendBufferSize(),
		/// as the system is free to adjust the value.

	virtual void setReceiveBufferSize(int size);
		/// Sets the size of the receive buffer.

	virtual int getReceiveBufferSize();
		/// Returns the size of the receive buffer.
		///
		/// The returned value may be different than the
		/// value previously set with setReceiveBufferSize(),
		/// as the system is free to adjust the value.

	virtual void setSendTimeout(const Poco::Timespan& timeout);
		/// Sets the send timeout for the socket.

	virtual Poco::Timespan getSendTimeout();
		/// Returns the send timeout for the socket.
		///
		/// The returned timeout may be different than the
		/// timeout previously set with setSendTimeout(),
		/// as the system is free to adjust the value.

	virtual void setReceiveTimeout(const Poco::Timespan& timeout);
		/// Sets the send timeout for the socket.
		///
		/// On systems that do not support SO_RCVTIMEO, a
		/// workaround using poll() is provided.

	virtual Poco::Timespan getReceiveTimeout();
		/// Returns the receive timeout for the socket.
		///
		/// The returned timeout may be different than the
		/// timeout previously set with setReceiveTimeout(),
		/// as the system is free to adjust the value.

	virtual SocketAddress address();
		/// Returns the IP address and port number of the socket.

	virtual SocketAddress peerAddress();
		/// Returns the IP address and port number of the peer socket.

	void setOption(int level, int option, int value);
		/// Sets the socket option specified by level and option
		/// to the given integer value.

	void setOption(int level, int option, unsigned value);
		/// Sets the socket option specified by level and option
		/// to the given integer value.

	void setOption(int level, int option, unsigned char value);
		/// Sets the socket option specified by level and option
		/// to the given integer value.

	void setOption(int level, int option, const Poco::Timespan& value);
		/// Sets the socket option specified by level and option
		/// to the given time value.

	void setOption(int level, int option, const IPAddress& value);
		/// Sets the socket option specified by level and option
		/// to the given time value.

	virtual void setRawOption(int level, int option, const void* value, poco_socklen_t length);
		/// Sets the socket option specified by level and option
		/// to the given time value.

	void getOption(int level, int option, int& value);
		/// Returns the value of the socket option
		/// specified by level and option.

	void getOption(int level, int option, unsigned& value);
		/// Returns the value of the socket option
		/// specified by level and option.

	void getOption(int level, int option, unsigned char& value);
		/// Returns the value of the socket option
		/// specified by level and option.

	void getOption(int level, int option, Poco::Timespan& value);
		/// Returns the value of the socket option
		/// specified by level and option.

	void getOption(int level, int option, IPAddress& value);
		/// Returns the value of the socket option
		/// specified by level and option.

	virtual void getRawOption(int level, int option, void* value, poco_socklen_t& length);
		/// Returns the value of the socket option
		/// specified by level and option.

	void setLinger(bool on, int seconds);
		/// Sets the value of the SO_LINGER socket option.

	void getLinger(bool& on, int& seconds);
		/// Returns the value of the SO_LINGER socket option.

	void setNoDelay(bool flag);
		/// Sets the value of the TCP_NODELAY socket option.

	bool getNoDelay();
		/// Returns the value of the TCP_NODELAY socket option.

	void setKeepAlive(bool flag);
		/// Sets the value of the SO_KEEPALIVE socket option.

	bool getKeepAlive();
		/// Returns the value of the SO_KEEPALIVE socket option.

	void setReuseAddress(bool flag);
		/// Sets the value of the SO_REUSEADDR socket option.

	bool getReuseAddress();
		/// Returns the value of the SO_REUSEADDR socket option.

	void setReusePort(bool flag);
		/// Sets the value of the SO_REUSEPORT socket option.
		/// Does nothing if the socket implementation does not
		/// support SO_REUSEPORT.

	bool getReusePort();
		/// Returns the value of the SO_REUSEPORT socket option.
		///
		/// Returns false if the socket implementation does not
		/// support SO_REUSEPORT.

	void setOOBInline(bool flag);
		/// Sets the value of the SO_OOBINLINE socket option.

	bool getOOBInline();
		/// Returns the value of the SO_OOBINLINE socket option.

	void setBroadcast(bool flag);
		/// Sets the value of the SO_BROADCAST socket option.

	bool getBroadcast();
		/// Returns the value of the SO_BROADCAST socket option.

	virtual void setBlocking(bool flag);
		/// Sets the socket in blocking mode if flag is true,
		/// disables blocking mode if flag is false.

	virtual bool getBlocking() const;
		/// Returns the blocking mode of the socket.
		/// This method will only work if the blocking modes of
		/// the socket are changed via the setBlocking method!

	virtual bool secure() const;
		/// Returns true iff the socket's connection is secure
		/// (using SSL or TLS).

	int socketError();
		/// Returns the value of the SO_ERROR socket option.

	poco_socket_t sockfd() const;
		/// Returns the socket descriptor for the
		/// underlying native socket.

	void ioctl(poco_ioctl_request_t request, int& arg);
		/// A wrapper for the ioctl system call.

	void ioctl(poco_ioctl_request_t request, void* arg);
		/// A wrapper for the ioctl system call.

#if defined(POCO_OS_FAMILY_UNIX)
	int fcntl(poco_fcntl_request_t request);
		/// A wrapper for the fcntl system call.

	int fcntl(poco_fcntl_request_t request, long arg);
		/// A wrapper for the fcntl system call.
#endif

	bool initialized() const;
		/// Returns true iff the underlying socket is initialized.

protected:
	SocketImpl();
		/// Creates a SocketImpl.

	SocketImpl(poco_socket_t sockfd);
		/// Creates a SocketImpl using the given native socket.

	virtual ~SocketImpl();
		/// Destroys the SocketImpl.
		/// Closes the socket if it is still open.

	virtual void init(int af);
		/// Creates the underlying native socket.
		///
		/// Subclasses must implement this method so
		/// that it calls initSocket() with the
		/// appropriate arguments.
		///
		/// The default implementation creates a
		/// stream socket.

	void initSocket(int af, int type, int proto = 0);
		/// Creates the underlying native socket.
		///
		/// The first argument, af, specifies the address family
		/// used by the socket, which should be either AF_INET or
		/// AF_INET6.
		///
		/// The second argument, type, specifies the type of the
		/// socket, which can be one of SOCK_STREAM, SOCK_DGRAM
		/// or SOCK_RAW.
		///
		/// The third argument, proto, is normally set to 0,
		/// except for raw sockets.

	void reset(poco_socket_t fd = POCO_INVALID_SOCKET);
		/// Allows subclasses to set the socket manually, iff no valid socket is set yet.

	static int lastError();
		/// Returns the last error code.

	static void error();
		/// Throws an appropriate exception for the last error.

	static void error(const std::string& arg);
		/// Throws an appropriate exception for the last error.

	static void error(int code);
		/// Throws an appropriate exception for the given error code.

	static void error(int code, const std::string& arg);
		/// Throws an appropriate exception for the given error code.

protected:
	SocketImpl(const SocketImpl&);
	SocketImpl& operator = (const SocketImpl&);

	poco_socket_t _sockfd;
	Poco::Timespan _recvTimeout;
	Poco::Timespan _sndTimeout;
	bool          _blocking;
	bool          _isBrokenTimeout;

	friend class Socket;
	friend class SecureSocketImpl;
	friend class PollSetImpl;
};


//
// inlines
//
inline poco_socket_t SocketImpl::sockfd() const
{
	return _sockfd;
}


inline bool SocketImpl::initialized() const
{
	return _sockfd != POCO_INVALID_SOCKET;
}


inline int SocketImpl::lastError()
{
#if defined(_WIN32)
	return WSAGetLastError();
#else
	return errno;
#endif
}


inline bool SocketImpl::getBlocking() const
{
	return _blocking;
}


} } // namespace Poco::Net


#endif // Net_SocketImpl_INCLUDED
