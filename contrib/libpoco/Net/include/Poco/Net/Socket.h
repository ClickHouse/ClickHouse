//
// Socket.h
//
// $Id: //poco/1.4/Net/include/Poco/Net/Socket.h#2 $
//
// Library: Net
// Package: Sockets
// Module:  Socket
//
// Definition of the Socket class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_Socket_INCLUDED
#define Net_Socket_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/SocketImpl.h"
#include <vector>


namespace Poco {
namespace Net {


class Net_API Socket
	/// Socket is the common base class for
	/// StreamSocket, ServerSocket, DatagramSocket and other
	/// socket classes.
	///
	/// It provides operations common to all socket types.
{
public:
	enum SelectMode
		/// The mode argument to poll() and select().
	{
		SELECT_READ  = 1,
		SELECT_WRITE = 2,
		SELECT_ERROR = 4
	};
	
	typedef std::vector<Socket> SocketList;

	Socket();
		/// Creates an uninitialized socket.

	Socket(const Socket& socket);
		/// Copy constructor.
		///
		/// Attaches the SocketImpl from the other socket and
		/// increments the reference count of the SocketImpl.
		
	Socket& operator = (const Socket& socket);
		/// Assignment operator.
		///
		/// Releases the socket's SocketImpl and
		/// attaches the SocketImpl from the other socket and
		/// increments the reference count of the SocketImpl.
		
	virtual ~Socket();
		/// Destroys the Socket and releases the
		/// SocketImpl.
		
	bool operator == (const Socket& socket) const;
		/// Returns true if both sockets share the same
		/// SocketImpl, false otherwise.

	bool operator != (const Socket& socket) const;
		/// Returns false if both sockets share the same
		/// SocketImpl, true otherwise.

	bool operator <  (const Socket& socket) const;
		/// Compares the SocketImpl pointers.
	
	bool operator <= (const Socket& socket) const;
		/// Compares the SocketImpl pointers.

	bool operator >  (const Socket& socket) const;
		/// Compares the SocketImpl pointers.

	bool operator >= (const Socket& socket) const;
		/// Compares the SocketImpl pointers.
		
	void close();
		/// Closes the socket.

	static int select(SocketList& readList, SocketList& writeList, SocketList& exceptList, const Poco::Timespan& timeout);
		/// Determines the status of one or more sockets, 
		/// using a call to select().
		///
		/// ReadList contains the list of sockets which should be
		/// checked for readability.
		///
		/// WriteList contains the list of sockets which should be
		/// checked for writeability.
		///
		/// ExceptList contains a list of sockets which should be
		/// checked for a pending error.
		///
		/// Returns the number of sockets ready.
		///
		/// After return, 
		///   * readList contains those sockets ready for reading,
		///   * writeList contains those sockets ready for writing,
		///   * exceptList contains those sockets with a pending error.
		///
		/// If the total number of sockets passed in readList, writeList and
		/// exceptList is zero, select() will return immediately and the
		/// return value will be 0.
		///
		/// If one of the sockets passed to select() is closed while
		/// select() runs, select will return immediately. However,
		/// the closed socket will not be included in any list.
		/// In this case, the return value may be greater than the sum
		/// of all sockets in all list.

	bool poll(const Poco::Timespan& timeout, int mode) const;
		/// Determines the status of the socket, using a 
		/// call to poll() or select().
		/// 
		/// The mode argument is constructed by combining the values
		/// of the SelectMode enumeration.
		///
		/// Returns true if the next operation corresponding to
		/// mode will not block, false otherwise.

	int available() const;
		/// Returns the number of bytes available that can be read
		/// without causing the socket to block.

	void setSendBufferSize(int size);
		/// Sets the size of the send buffer.
		
	int getSendBufferSize() const;
		/// Returns the size of the send buffer.
		///
		/// The returned value may be different than the
		/// value previously set with setSendBufferSize(),
		/// as the system is free to adjust the value.

	void setReceiveBufferSize(int size);
		/// Sets the size of the receive buffer.
		
	int getReceiveBufferSize() const;
		/// Returns the size of the receive buffer.
		///
		/// The returned value may be different than the
		/// value previously set with setReceiveBufferSize(),
		/// as the system is free to adjust the value.

	void setSendTimeout(const Poco::Timespan& timeout);
		/// Sets the send timeout for the socket.
	
	Poco::Timespan getSendTimeout() const;
		/// Returns the send timeout for the socket.
		///
		/// The returned timeout may be different than the
		/// timeout previously set with setSendTimeout(),
		/// as the system is free to adjust the value.

	void setReceiveTimeout(const Poco::Timespan& timeout);
		/// Sets the send timeout for the socket.
		///
		/// On systems that do not support SO_RCVTIMEO, a
		/// workaround using poll() is provided.
	
	Poco::Timespan getReceiveTimeout() const;
		/// Returns the receive timeout for the socket.
		///
		/// The returned timeout may be different than the
		/// timeout previously set with getReceiveTimeout(),
		/// as the system is free to adjust the value.

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

	void getOption(int level, int option, int& value) const;
		/// Returns the value of the socket option 
		/// specified by level and option.

	void getOption(int level, int option, unsigned& value) const;
		/// Returns the value of the socket option 
		/// specified by level and option.

	void getOption(int level, int option, unsigned char& value) const;
		/// Returns the value of the socket option 
		/// specified by level and option.

	void getOption(int level, int option, Poco::Timespan& value) const;
		/// Returns the value of the socket option 
		/// specified by level and option.
	
	void getOption(int level, int option, IPAddress& value) const;
		/// Returns the value of the socket option 
		/// specified by level and option.

	void setLinger(bool on, int seconds);
		/// Sets the value of the SO_LINGER socket option.
		
	void getLinger(bool& on, int& seconds) const;
		/// Returns the value of the SO_LINGER socket option.
	
	void setNoDelay(bool flag);
		/// Sets the value of the TCP_NODELAY socket option.
		
	bool getNoDelay() const;
		/// Returns the value of the TCP_NODELAY socket option.
	
	void setKeepAlive(bool flag);
		/// Sets the value of the SO_KEEPALIVE socket option.
		
	bool getKeepAlive() const;
		/// Returns the value of the SO_KEEPALIVE socket option.
	
	void setReuseAddress(bool flag);
		/// Sets the value of the SO_REUSEADDR socket option.
	
	bool getReuseAddress() const;
		/// Returns the value of the SO_REUSEADDR socket option.

	void setReusePort(bool flag);
		/// Sets the value of the SO_REUSEPORT socket option.
		/// Does nothing if the socket implementation does not
		/// support SO_REUSEPORT.
	
	bool getReusePort() const;
		/// Returns the value of the SO_REUSEPORT socket option.
		///
		/// Returns false if the socket implementation does not
		/// support SO_REUSEPORT.
		
	void setOOBInline(bool flag);
		/// Sets the value of the SO_OOBINLINE socket option.
	
	bool getOOBInline() const;
		/// Returns the value of the SO_OOBINLINE socket option.

	void setBlocking(bool flag);
		/// Sets the socket in blocking mode if flag is true,
		/// disables blocking mode if flag is false.

	bool getBlocking() const;
		/// Returns the blocking mode of the socket.
		/// This method will only work if the blocking modes of 
		/// the socket are changed via the setBlocking method!

	SocketAddress address() const;
		/// Returns the IP address and port number of the socket.
		
	SocketAddress peerAddress() const;
		/// Returns the IP address and port number of the peer socket.

	SocketImpl* impl() const;
		/// Returns the SocketImpl for this socket.
		
	bool secure() const;
		/// Returns true iff the socket's connection is secure
		/// (using SSL or TLS).
		
	static bool supportsIPv4();
		/// Returns true if the system supports IPv4.
		
	static bool supportsIPv6();
		/// Returns true if the system supports IPv6.

	void init(int af);
		/// Creates the underlying system socket for the given
		/// address family.
		///
		/// Normally, this method should not be called directly, as
		/// socket creation will be handled automatically. There are
		/// a few situations where calling this method after creation
		/// of the Socket object makes sense. One example is setting
		/// a socket option before calling bind() on a ServerSocket.

protected:
	Socket(SocketImpl* pImpl);
		/// Creates the Socket and attaches the given SocketImpl.
		/// The socket takes owership of the SocketImpl.

	poco_socket_t sockfd() const;
		/// Returns the socket descriptor for this socket.

private:

#if defined(POCO_HAVE_FD_POLL)
class FDCompare
	/// Utility functor used to compare socket file descriptors.
	/// Used in poll() member function.
{
public:
	FDCompare(int fd): _fd(fd) { }
	inline bool operator()(const Socket& socket) const
	{ return socket.sockfd() == _fd; }

private:
	FDCompare();
	int _fd;
};
#endif

	SocketImpl* _pImpl;
};


//
// inlines
//
inline bool Socket::operator == (const Socket& socket) const
{
	return _pImpl == socket._pImpl;
}


inline bool Socket::operator != (const Socket& socket) const
{
	return _pImpl != socket._pImpl;
}


inline bool Socket::operator <  (const Socket& socket) const
{
	return _pImpl < socket._pImpl;
}


inline bool Socket::operator <= (const Socket& socket) const
{
	return _pImpl <= socket._pImpl;
}


inline bool Socket::operator >  (const Socket& socket) const
{
	return _pImpl > socket._pImpl;
}


inline bool Socket::operator >= (const Socket& socket) const
{
	return _pImpl >= socket._pImpl;
}


inline void Socket::close()
{
	_pImpl->close();
}


inline bool Socket::poll(const Poco::Timespan& timeout, int mode) const
{
	return _pImpl->poll(timeout, mode);
}


inline int Socket::available() const
{
	return _pImpl->available();
}


inline void Socket::setSendBufferSize(int size)
{
	_pImpl->setSendBufferSize(size);
}

	
inline int Socket::getSendBufferSize() const
{
	return _pImpl->getSendBufferSize();
}


inline void Socket::setReceiveBufferSize(int size)
{
	_pImpl->setReceiveBufferSize(size);
}

	
inline int Socket::getReceiveBufferSize() const
{
	return _pImpl->getReceiveBufferSize();
}


inline void Socket::setSendTimeout(const Poco::Timespan& timeout)
{
	_pImpl->setSendTimeout(timeout);
}


inline Poco::Timespan Socket::getSendTimeout() const
{
	return _pImpl->getSendTimeout();
}


inline void Socket::setReceiveTimeout(const Poco::Timespan& timeout)
{
	_pImpl->setReceiveTimeout(timeout);
}


inline Poco::Timespan Socket::getReceiveTimeout() const
{
	return _pImpl->getReceiveTimeout();
}


inline void Socket::setOption(int level, int option, int value)
{
	_pImpl->setOption(level, option, value);
}


inline void Socket::setOption(int level, int option, unsigned value)
{
	_pImpl->setOption(level, option, value);
}


inline void Socket::setOption(int level, int option, unsigned char value)
{
	_pImpl->setOption(level, option, value);
}


inline void Socket::setOption(int level, int option, const Poco::Timespan& value)
{
	_pImpl->setOption(level, option, value);
}

	
inline void Socket::setOption(int level, int option, const IPAddress& value)
{
	_pImpl->setOption(level, option, value);
}


inline void Socket::getOption(int level, int option, int& value) const
{
	_pImpl->getOption(level, option, value);
}


inline void Socket::getOption(int level, int option, unsigned& value) const
{
	_pImpl->getOption(level, option, value);
}


inline void Socket::getOption(int level, int option, unsigned char& value) const
{
	_pImpl->getOption(level, option, value);
}


inline void Socket::getOption(int level, int option, Poco::Timespan& value) const
{
	_pImpl->getOption(level, option, value);
}


inline void Socket::getOption(int level, int option, IPAddress& value) const
{
	_pImpl->getOption(level, option, value);
}


inline void Socket::setLinger(bool on, int seconds)
{
	_pImpl->setLinger(on, seconds);
}

	
inline void Socket::getLinger(bool& on, int& seconds) const
{
	_pImpl->getLinger(on, seconds);
}


inline void Socket::setNoDelay(bool flag)
{
	_pImpl->setNoDelay(flag);
}

	
inline bool Socket::getNoDelay() const
{
	return _pImpl->getNoDelay();
}


inline void Socket::setKeepAlive(bool flag)
{
	_pImpl->setKeepAlive(flag);
}

	
inline bool Socket::getKeepAlive() const
{
	return _pImpl->getKeepAlive();
}


inline void Socket::setReuseAddress(bool flag)
{
	_pImpl->setReuseAddress(flag);
}


inline bool Socket::getReuseAddress() const
{
	return _pImpl->getReuseAddress();
}


inline void Socket::setReusePort(bool flag)
{
	_pImpl->setReusePort(flag);
}


inline bool Socket::getReusePort() const
{
	return _pImpl->getReusePort();
}

	
inline void Socket::setOOBInline(bool flag)
{
	_pImpl->setOOBInline(flag);
}


inline bool Socket::getOOBInline() const
{
	return _pImpl->getOOBInline();
}


inline void Socket::setBlocking(bool flag)
{
	_pImpl->setBlocking(flag);
}


inline bool Socket::getBlocking() const
{
	return _pImpl->getBlocking();
}


inline SocketImpl* Socket::impl() const
{
	return _pImpl;
}


inline poco_socket_t Socket::sockfd() const
{
	return _pImpl->sockfd();
}


inline SocketAddress Socket::address() const
{
	return _pImpl->address();
}

	
inline SocketAddress Socket::peerAddress() const
{
	return _pImpl->peerAddress();
}


inline bool Socket::secure() const
{
	return _pImpl->secure();
}


inline bool Socket::supportsIPv4()
{
	return true;
}


inline bool Socket::supportsIPv6()
{
#if defined(POCO_HAVE_IPv6)
	return true;
#else
	return false;
#endif
}


inline void Socket::init(int af)
{
	_pImpl->init(af);
}


} } // namespace Poco::Net


#endif // Net_Socket_INCLUDED
