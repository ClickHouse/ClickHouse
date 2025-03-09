//
// SocketImpl.cpp
//
// Library: Net
// Package: Sockets
// Module:  SocketImpl
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/SocketImpl.h"
#include "Poco/Net/NetException.h"
#include "Poco/Net/StreamSocketImpl.h"
#include "Poco/NumberFormatter.h"
#include "Poco/Timestamp.h"
#include "Poco/ErrorHandler.h"
#include <string.h> // FD_SET needs memset on some platforms, so we can't use <cstring>


#if   defined(POCO_OS_FAMILY_UNIX)
#ifndef POCO_HAVE_FD_POLL
#define POCO_HAVE_FD_POLL 1
#endif
#endif


#if defined(POCO_HAVE_FD_POLL)
#include <poll.h>
#endif


#if defined(sun) || defined(__sun) || defined(__sun__)
#include <unistd.h>
#include <stropts.h>
#endif




using Poco::IOException;
using Poco::TimeoutException;
using Poco::InvalidArgumentException;
using Poco::NumberFormatter;
using Poco::Timespan;


namespace Poco {
namespace Net {


bool checkIsBrokenTimeout()
{
#if defined(POCO_BROKEN_TIMEOUTS)
	return true;
#endif
	return false;
}


SocketImpl::SocketImpl():
	_sockfd(POCO_INVALID_SOCKET),
	_blocking(true),
	_isBrokenTimeout(checkIsBrokenTimeout())
{
}


SocketImpl::SocketImpl(poco_socket_t sockfd):
	_sockfd(sockfd),
	_blocking(true),
	_isBrokenTimeout(checkIsBrokenTimeout())
{
}


SocketImpl::~SocketImpl()
{
	close();
}


SocketImpl* SocketImpl::acceptConnection(SocketAddress& clientAddr)
{
	if (_sockfd == POCO_INVALID_SOCKET) throw InvalidSocketException();

	sockaddr_storage buffer;
	struct sockaddr* pSA = reinterpret_cast<struct sockaddr*>(&buffer);
	poco_socklen_t saLen = sizeof(buffer);
	poco_socket_t sd;
	do
	{
		sd = ::accept(_sockfd, pSA, &saLen);
	}
	while (sd == POCO_INVALID_SOCKET && lastError() == POCO_EINTR);
	if (sd != POCO_INVALID_SOCKET)
	{
		clientAddr = SocketAddress(pSA, saLen);
		return new StreamSocketImpl(sd);
	}
	error(); // will throw
	return 0;
}


void SocketImpl::connect(const SocketAddress& address)
{
	if (_sockfd == POCO_INVALID_SOCKET)
	{
		init(address.af());
	}
	int rc;
	do
	{
		rc = ::connect(_sockfd, address.addr(), address.length());
	}
	while (rc != 0 && lastError() == POCO_EINTR);
	if (rc != 0)
	{
		int err = lastError();
		error(err, address.toString());
	}
}


void SocketImpl::connect(const SocketAddress& address, const Poco::Timespan& timeout)
{
	if (_sockfd == POCO_INVALID_SOCKET)
	{
		init(address.af());
	}
	setBlocking(false);
	try
	{
		int rc = ::connect(_sockfd, address.addr(), address.length());
		if (rc != 0)
		{
			int err = lastError();
			if (err != POCO_EINPROGRESS && err != POCO_EWOULDBLOCK)
				error(err, address.toString());
			if (!poll(timeout, SELECT_READ | SELECT_WRITE | SELECT_ERROR))
				throw Poco::TimeoutException("connect timed out", address.toString());
			err = socketError();
			if (err != 0) error(err);
		}
	}
	catch (Poco::Exception&)
	{
		setBlocking(true);
		throw;
	}
	setBlocking(true);
}


void SocketImpl::connectNB(const SocketAddress& address)
{
	if (_sockfd == POCO_INVALID_SOCKET)
	{
		init(address.af());
	}
	setBlocking(false);
	int rc = ::connect(_sockfd, address.addr(), address.length());
	if (rc != 0)
	{
		int err = lastError();
		if (err != POCO_EINPROGRESS && err != POCO_EWOULDBLOCK)
			error(err, address.toString());
	}
}


void SocketImpl::bind(const SocketAddress& address, bool reuseAddress)
{
	bind(address, reuseAddress, reuseAddress);
}


void SocketImpl::bind(const SocketAddress& address, bool reuseAddress, bool reusePort)
{
	if (_sockfd == POCO_INVALID_SOCKET)
	{
		init(address.af());
	}
	if (reuseAddress)
		setReuseAddress(true);
    if (reusePort)
		setReusePort(true);
	int rc = ::bind(_sockfd, address.addr(), address.length());
	if (rc != 0) error(address.toString());
}


void SocketImpl::bind6(const SocketAddress& address, bool reuseAddress, bool ipV6Only)
{
    bind6(address, reuseAddress, true, ipV6Only);
}


void SocketImpl::bind6(const SocketAddress& address, bool reuseAddress, bool reusePort, bool ipV6Only)
{
#if defined(POCO_HAVE_IPv6)
	if (address.family() != SocketAddress::IPv6)
		throw Poco::InvalidArgumentException("SocketAddress must be an IPv6 address");

	if (_sockfd == POCO_INVALID_SOCKET)
	{
		init(address.af());
	}
#ifdef IPV6_V6ONLY
	setOption(IPPROTO_IPV6, IPV6_V6ONLY, ipV6Only ? 1 : 0);
#else
	if (ipV6Only) throw Poco::NotImplementedException("IPV6_V6ONLY not defined.");
#endif
	if (reuseAddress)
		setReuseAddress(true);
    if (reusePort)
		setReusePort(true);
	int rc = ::bind(_sockfd, address.addr(), address.length());
	if (rc != 0) error(address.toString());
#else
	throw Poco::NotImplementedException("No IPv6 support available");
#endif
}


void SocketImpl::listen(int backlog)
{
	if (_sockfd == POCO_INVALID_SOCKET) throw InvalidSocketException();

	int rc = ::listen(_sockfd, backlog);
	if (rc != 0) error();
}


void SocketImpl::close()
{
	if (_sockfd != POCO_INVALID_SOCKET)
	{
		poco_closesocket(_sockfd);
		_sockfd = POCO_INVALID_SOCKET;
	}
}


void SocketImpl::shutdownReceive()
{
	if (_sockfd == POCO_INVALID_SOCKET) throw InvalidSocketException();

	int rc = ::shutdown(_sockfd, 0);
	if (rc != 0) error();
}


void SocketImpl::shutdownSend()
{
	if (_sockfd == POCO_INVALID_SOCKET) throw InvalidSocketException();

	int rc = ::shutdown(_sockfd, 1);
	if (rc != 0) error();
}


void SocketImpl::shutdown()
{
	if (_sockfd == POCO_INVALID_SOCKET) throw InvalidSocketException();

	int rc = ::shutdown(_sockfd, 2);
	if (rc != 0) error();
}


int SocketImpl::sendBytes(const void* buffer, int length, int flags)
{
    bool blocking = _blocking && (flags & MSG_DONTWAIT) == 0;

	if (_isBrokenTimeout && blocking)
	{
		if (_sndTimeout.totalMicroseconds() != 0)
		{
			if (!poll(_sndTimeout, SELECT_WRITE))
				throw TimeoutException();
		}
	}

	int rc;
	do
	{
		if (_sockfd == POCO_INVALID_SOCKET) throw InvalidSocketException();
		rc = ::send(_sockfd, reinterpret_cast<const char*>(buffer), length, flags);
	}
	while (blocking && rc < 0 && lastError() == POCO_EINTR);
	if (rc < 0)
	{
		int err = lastError();
		if ((err == POCO_EAGAIN || err == POCO_EWOULDBLOCK) && !blocking)
			;
		else if (err == POCO_EAGAIN || err == POCO_ETIMEDOUT)
			throw TimeoutException();
		else
			error(err);
	}
	return rc;
}


int SocketImpl::receiveBytes(void* buffer, int length, int flags)
{
	bool blocking = _blocking && (flags & MSG_DONTWAIT) == 0;
	if (_isBrokenTimeout && blocking)
	{
		if (_recvTimeout.totalMicroseconds() != 0)
		{
			if (!poll(_recvTimeout, SELECT_READ))
				throw TimeoutException();
		}
	}

	int rc;
	do
	{
		if (_sockfd == POCO_INVALID_SOCKET) throw InvalidSocketException();
		rc = ::recv(_sockfd, reinterpret_cast<char*>(buffer), length, flags);
	}
	while (blocking && rc < 0 && lastError() == POCO_EINTR);
	if (rc < 0)
	{
		int err = lastError();
		if ((err == POCO_EAGAIN || err == POCO_EWOULDBLOCK) && !blocking)
			;
		else if (err == POCO_EAGAIN || err == POCO_ETIMEDOUT)
			throw TimeoutException(err);
		else
			error(err);
	}
	return rc;
}


int SocketImpl::sendTo(const void* buffer, int length, const SocketAddress& address, int flags)
{
	int rc;
	do
	{
		if (_sockfd == POCO_INVALID_SOCKET) throw InvalidSocketException();
		rc = ::sendto(_sockfd, reinterpret_cast<const char*>(buffer), length, flags, address.addr(), address.length());
	}
	while (_blocking && rc < 0 && lastError() == POCO_EINTR);
	if (rc < 0) error();
	return rc;
}


int SocketImpl::receiveFrom(void* buffer, int length, SocketAddress& address, int flags)
{
	if (_isBrokenTimeout)
	{
		if (_recvTimeout.totalMicroseconds() != 0)
		{
			if (!poll(_recvTimeout, SELECT_READ))
				throw TimeoutException();
		}
	}

	sockaddr_storage abuffer;
	struct sockaddr* pSA = reinterpret_cast<struct sockaddr*>(&abuffer);
	poco_socklen_t saLen = sizeof(abuffer);
	int rc;
	do
	{
		if (_sockfd == POCO_INVALID_SOCKET) throw InvalidSocketException();
		rc = ::recvfrom(_sockfd, reinterpret_cast<char*>(buffer), length, flags, pSA, &saLen);
	}
	while (_blocking && rc < 0 && lastError() == POCO_EINTR);
	if (rc >= 0)
	{
		address = SocketAddress(pSA, saLen);
	}
	else
	{
		int err = lastError();
		if (err == POCO_EAGAIN && !_blocking)
			;
		else if (err == POCO_EAGAIN || err == POCO_ETIMEDOUT)
			throw TimeoutException(err);
		else
			error(err);
	}
	return rc;
}


void SocketImpl::sendUrgent(unsigned char data)
{
	if (_sockfd == POCO_INVALID_SOCKET) throw InvalidSocketException();

	int rc = ::send(_sockfd, reinterpret_cast<const char*>(&data), sizeof(data), MSG_OOB);
	if (rc < 0) error();
}


int SocketImpl::available()
{
	int result = 0;
	ioctl(FIONREAD, result);
	return result;
}


bool SocketImpl::secure() const
{
	return false;
}


bool SocketImpl::pollImpl(Poco::Timespan& remainingTime, int mode)
{
	poco_socket_t sockfd = _sockfd;
	if (sockfd == POCO_INVALID_SOCKET) throw InvalidSocketException();

#if defined(POCO_HAVE_FD_POLL)

	pollfd pollBuf;

	memset(&pollBuf, 0, sizeof(pollfd));
	pollBuf.fd = _sockfd;
	if (mode & SELECT_READ) pollBuf.events |= POLLIN;
	if (mode & SELECT_WRITE) pollBuf.events |= POLLOUT;

	int rc;
	do
	{
		Poco::Timestamp start;
		rc = ::poll(&pollBuf, 1, remainingTime.totalMilliseconds());
		/// Decrease timeout in case of retriable error.
		///
		/// But do this only if the timeout is positive,
		/// since negative timeout means an infinite timeout.
		if (rc < 0 && lastError() == POCO_EINTR && remainingTime > 0)
		{
			Poco::Timestamp end;
			Poco::Timespan waited = end - start;
			if (waited < remainingTime)
				remainingTime -= waited;
			else
				remainingTime = 0;
		}
	}
	while (rc < 0 && lastError() == POCO_EINTR);
	if (rc < 0) error();
	return rc > 0;

#else

	fd_set fdRead;
	fd_set fdWrite;
	fd_set fdExcept;
	FD_ZERO(&fdRead);
	FD_ZERO(&fdWrite);
	FD_ZERO(&fdExcept);
	if (mode & SELECT_READ)
	{
		FD_SET(sockfd, &fdRead);
	}
	if (mode & SELECT_WRITE)
	{
		FD_SET(sockfd, &fdWrite);
	}
	if (mode & SELECT_ERROR)
	{
		FD_SET(sockfd, &fdExcept);
	}
	int errorCode = POCO_ENOERR;
	int rc;
	do
	{
		struct timeval tv;
		tv.tv_sec  = (long) remainingTime.totalSeconds();
		tv.tv_usec = (long) remainingTime.useconds();
		Poco::Timestamp start;
		rc = ::select(int(sockfd) + 1, &fdRead, &fdWrite, &fdExcept, &tv);
		if (rc < 0 && (errorCode = lastError()) == POCO_EINTR)
		{
			Poco::Timestamp end;
			Poco::Timespan waited = end - start;
			if (waited < remainingTime)
				remainingTime -= waited;
			else
				remainingTime = 0;
		}
	}
	while (rc < 0 && errorCode == POCO_EINTR);
	if (rc < 0) error(errorCode);
	return rc > 0;

#endif // POCO_HAVE_FD_POLL
}

bool SocketImpl::poll(const Poco::Timespan& timeout, int mode)
{
	Poco::Timespan remainingTime(timeout);
	return pollImpl(remainingTime, mode);
}

void SocketImpl::setSendBufferSize(int size)
{
	setOption(SOL_SOCKET, SO_SNDBUF, size);
}


int SocketImpl::getSendBufferSize()
{
	int result;
	getOption(SOL_SOCKET, SO_SNDBUF, result);
	return result;
}


void SocketImpl::setReceiveBufferSize(int size)
{
	setOption(SOL_SOCKET, SO_RCVBUF, size);
}


int SocketImpl::getReceiveBufferSize()
{
	int result;
	getOption(SOL_SOCKET, SO_RCVBUF, result);
	return result;
}


void SocketImpl::setSendTimeout(const Poco::Timespan& timeout)
{
	setOption(SOL_SOCKET, SO_SNDTIMEO, timeout);
	_sndTimeout = timeout;
}


Poco::Timespan SocketImpl::getSendTimeout()
{
	Timespan result;
#if   !defined(POCO_BROKEN_TIMEOUTS)
	getOption(SOL_SOCKET, SO_SNDTIMEO, result);
#endif
	if (_isBrokenTimeout)
		result = _sndTimeout;
	return result;
}


void SocketImpl::setReceiveTimeout(const Poco::Timespan& timeout)
{
	setOption(SOL_SOCKET, SO_RCVTIMEO, timeout);
	_recvTimeout = timeout;
}


Poco::Timespan SocketImpl::getReceiveTimeout()
{
	Timespan result;
#if   !defined(POCO_BROKEN_TIMEOUTS)
	getOption(SOL_SOCKET, SO_RCVTIMEO, result);
#endif
	if (_isBrokenTimeout)
		result = _recvTimeout;
	return result;
}


SocketAddress SocketImpl::address()
{
	if (_sockfd == POCO_INVALID_SOCKET) throw InvalidSocketException();

	sockaddr_storage buffer;
	struct sockaddr* pSA = reinterpret_cast<struct sockaddr*>(&buffer);
	poco_socklen_t saLen = sizeof(buffer);
	int rc = ::getsockname(_sockfd, pSA, &saLen);
	if (rc == 0)
		return SocketAddress(pSA, saLen);
	else
		error();
	return SocketAddress();
}


SocketAddress SocketImpl::peerAddress()
{
	if (_sockfd == POCO_INVALID_SOCKET) throw InvalidSocketException();

	sockaddr_storage buffer;
	struct sockaddr* pSA = reinterpret_cast<struct sockaddr*>(&buffer);
	poco_socklen_t saLen = sizeof(buffer);
	int rc = ::getpeername(_sockfd, pSA, &saLen);
	if (rc == 0)
		return SocketAddress(pSA, saLen);
	else
		error();
	return SocketAddress();
}


void SocketImpl::setOption(int level, int option, int value)
{
	setRawOption(level, option, &value, sizeof(value));
}


void SocketImpl::setOption(int level, int option, unsigned value)
{
	setRawOption(level, option, &value, sizeof(value));
}


void SocketImpl::setOption(int level, int option, unsigned char value)
{
	setRawOption(level, option, &value, sizeof(value));
}


void SocketImpl::setOption(int level, int option, const IPAddress& value)
{
	setRawOption(level, option, value.addr(), value.length());
}


void SocketImpl::setOption(int level, int option, const Poco::Timespan& value)
{
	struct timeval tv;
	tv.tv_sec  = (long) value.totalSeconds();
	tv.tv_usec = (long) value.useconds();

	setRawOption(level, option, &tv, sizeof(tv));
}


void SocketImpl::setRawOption(int level, int option, const void* value, poco_socklen_t length)
{
	if (_sockfd == POCO_INVALID_SOCKET) throw InvalidSocketException();

	int rc = ::setsockopt(_sockfd, level, option, reinterpret_cast<const char*>(value), length);
	if (rc == -1) error();
}


void SocketImpl::getOption(int level, int option, int& value)
{
	poco_socklen_t len = sizeof(value);
	getRawOption(level, option, &value, len);
}


void SocketImpl::getOption(int level, int option, unsigned& value)
{
	poco_socklen_t len = sizeof(value);
	getRawOption(level, option, &value, len);
}


void SocketImpl::getOption(int level, int option, unsigned char& value)
{
	poco_socklen_t len = sizeof(value);
	getRawOption(level, option, &value, len);
}


void SocketImpl::getOption(int level, int option, Poco::Timespan& value)
{
	struct timeval tv;
	poco_socklen_t len = sizeof(tv);
	getRawOption(level, option, &tv, len);
	value.assign(tv.tv_sec, tv.tv_usec);
}


void SocketImpl::getOption(int level, int option, IPAddress& value)
{
	char buffer[IPAddress::MAX_ADDRESS_LENGTH];
	poco_socklen_t len = sizeof(buffer);
	getRawOption(level, option, buffer, len);
	value = IPAddress(buffer, len);
}


void SocketImpl::getRawOption(int level, int option, void* value, poco_socklen_t& length)
{
	if (_sockfd == POCO_INVALID_SOCKET) throw InvalidSocketException();

	int rc = ::getsockopt(_sockfd, level, option, reinterpret_cast<char*>(value), &length);
	if (rc == -1) error();
}


void SocketImpl::setLinger(bool on, int seconds)
{
	struct linger l;
	l.l_onoff  = on ? 1 : 0;
	l.l_linger = seconds;
	setRawOption(SOL_SOCKET, SO_LINGER, &l, sizeof(l));
}


void SocketImpl::getLinger(bool& on, int& seconds)
{
	struct linger l;
	poco_socklen_t len = sizeof(l);
	getRawOption(SOL_SOCKET, SO_LINGER, &l, len);
	on      = l.l_onoff != 0;
	seconds = l.l_linger;
}


void SocketImpl::setNoDelay(bool flag)
{
	int value = flag ? 1 : 0;
	setOption(IPPROTO_TCP, TCP_NODELAY, value);
}


bool SocketImpl::getNoDelay()
{
	int value(0);
	getOption(IPPROTO_TCP, TCP_NODELAY, value);
	return value != 0;
}


void SocketImpl::setKeepAlive(bool flag)
{
	int value = flag ? 1 : 0;
	setOption(SOL_SOCKET, SO_KEEPALIVE, value);
}


bool SocketImpl::getKeepAlive()
{
	int value(0);
	getOption(SOL_SOCKET, SO_KEEPALIVE, value);
	return value != 0;
}


void SocketImpl::setReuseAddress(bool flag)
{
	int value = flag ? 1 : 0;
	setOption(SOL_SOCKET, SO_REUSEADDR, value);
}


bool SocketImpl::getReuseAddress()
{
	int value(0);
	getOption(SOL_SOCKET, SO_REUSEADDR, value);
	return value != 0;
}


void SocketImpl::setReusePort(bool flag)
{
#ifdef SO_REUSEPORT
	try
	{
		int value = flag ? 1 : 0;
		setOption(SOL_SOCKET, SO_REUSEPORT, value);
	}
	catch (IOException&)
	{
		// ignore error, since not all implementations
		// support SO_REUSEPORT, even if the macro
		// is defined.
	}
#endif
}


bool SocketImpl::getReusePort()
{
#ifdef SO_REUSEPORT
	int value(0);
	getOption(SOL_SOCKET, SO_REUSEPORT, value);
	return value != 0;
#else
	return false;
#endif
}


void SocketImpl::setOOBInline(bool flag)
{
	int value = flag ? 1 : 0;
	setOption(SOL_SOCKET, SO_OOBINLINE, value);
}


bool SocketImpl::getOOBInline()
{
	int value(0);
	getOption(SOL_SOCKET, SO_OOBINLINE, value);
	return value != 0;
}


void SocketImpl::setBroadcast(bool flag)
{
	int value = flag ? 1 : 0;
	setOption(SOL_SOCKET, SO_BROADCAST, value);
}


bool SocketImpl::getBroadcast()
{
	int value(0);
	getOption(SOL_SOCKET, SO_BROADCAST, value);
	return value != 0;
}


void SocketImpl::setBlocking(bool flag)
{
#if !defined(POCO_OS_FAMILY_UNIX)
	int arg = flag ? 0 : 1;
	ioctl(FIONBIO, arg);
#else
	int arg = fcntl(F_GETFL);
	long flags = arg & ~O_NONBLOCK;
	if (!flag) flags |= O_NONBLOCK;
	(void) fcntl(F_SETFL, flags);
#endif
	_blocking = flag;
}


int SocketImpl::socketError()
{
	int result(0);
	getOption(SOL_SOCKET, SO_ERROR, result);
	return result;
}


void SocketImpl::init(int af)
{
#ifdef SOCK_CLOEXEC
	initSocket(af, SOCK_STREAM|SOCK_CLOEXEC);
#else
	initSocket(af, SOCK_STREAM);
#endif
}


void SocketImpl::initSocket(int af, int type, int proto)
{
	poco_assert (_sockfd == POCO_INVALID_SOCKET);

	_sockfd = ::socket(af, type, proto);
	if (_sockfd == POCO_INVALID_SOCKET)
		error();

#if defined(__MACH__) && defined(__APPLE__) || defined(__FreeBSD__)
	// SIGPIPE sends a signal that if unhandled (which is the default)
	// will crash the process. This only happens on UNIX, and not Linux.
	//
	// In order to have POCO sockets behave the same across platforms, it is
	// best to just ignore SIGPIPE altogether.
	setOption(SOL_SOCKET, SO_NOSIGPIPE, 1);
#endif
}


void SocketImpl::ioctl(poco_ioctl_request_t request, int& arg)
{
	int rc = ::ioctl(_sockfd, request, &arg);
	if (rc != 0) error();
}


void SocketImpl::ioctl(poco_ioctl_request_t request, void* arg)
{
	int rc = ::ioctl(_sockfd, request, arg);
	if (rc != 0) error();
}


#if defined(POCO_OS_FAMILY_UNIX)
int SocketImpl::fcntl(poco_fcntl_request_t request)
{
	int rc = ::fcntl(_sockfd, request);
	if (rc == -1) error();
	return rc;
}


int SocketImpl::fcntl(poco_fcntl_request_t request, long arg)
{
	int rc = ::fcntl(_sockfd, request, arg);
	if (rc == -1) error();
	return rc;
}
#endif


void SocketImpl::reset(poco_socket_t aSocket)
{
	_sockfd = aSocket;
}


void SocketImpl::error()
{
	int err = lastError();
	std::string empty;
	error(err, empty);
}


void SocketImpl::error(const std::string& arg)
{
	error(lastError(), arg);
}


void SocketImpl::error(int code)
{
	std::string arg;
	error(code, arg);
}


void SocketImpl::error(int code, const std::string& arg)
{
	switch (code)
	{
	case POCO_ENOERR: return;
	case POCO_ESYSNOTREADY:
		throw NetException("Net subsystem not ready", code);
	case POCO_ENOTINIT:
		throw NetException("Net subsystem not initialized", code);
	case POCO_EINTR:
		throw IOException("Interrupted", code);
	case POCO_EACCES:
		throw IOException("Permission denied", code);
	case POCO_EFAULT:
		throw IOException("Bad address", code);
	case POCO_EINVAL:
		throw InvalidArgumentException(code);
	case POCO_EMFILE:
		throw IOException("Too many open files", code);
	case POCO_EWOULDBLOCK:
		throw IOException("Operation would block", code);
	case POCO_EINPROGRESS:
		throw IOException("Operation now in progress", code);
	case POCO_EALREADY:
		throw IOException("Operation already in progress", code);
	case POCO_ENOTSOCK:
		throw IOException("Socket operation attempted on non-socket", code);
	case POCO_EDESTADDRREQ:
		throw NetException("Destination address required", code);
	case POCO_EMSGSIZE:
		throw NetException("Message too long", code);
	case POCO_EPROTOTYPE:
		throw NetException("Wrong protocol type", code);
	case POCO_ENOPROTOOPT:
		throw NetException("Protocol not available", code);
	case POCO_EPROTONOSUPPORT:
		throw NetException("Protocol not supported", code);
	case POCO_ESOCKTNOSUPPORT:
		throw NetException("Socket type not supported", code);
	case POCO_ENOTSUP:
		throw NetException("Operation not supported", code);
	case POCO_EPFNOSUPPORT:
		throw NetException("Protocol family not supported", code);
	case POCO_EAFNOSUPPORT:
		throw NetException("Address family not supported", code);
	case POCO_EADDRINUSE:
		throw NetException("Address already in use", arg, code);
	case POCO_EADDRNOTAVAIL:
		throw NetException("Cannot assign requested address", arg, code);
	case POCO_ENETDOWN:
		throw NetException("Network is down", code);
	case POCO_ENETUNREACH:
		throw NetException("Network is unreachable", code);
	case POCO_ENETRESET:
		throw NetException("Network dropped connection on reset", code);
	case POCO_ECONNABORTED:
		throw ConnectionAbortedException(code);
	case POCO_ECONNRESET:
		throw ConnectionResetException(code);
	case POCO_ENOBUFS:
		throw IOException("No buffer space available", code);
	case POCO_EISCONN:
		throw NetException("Socket is already connected", code);
	case POCO_ENOTCONN:
		throw NetException("Socket is not connected", code);
	case POCO_ESHUTDOWN:
		throw NetException("Cannot send after socket shutdown", code);
	case POCO_ETIMEDOUT:
		throw TimeoutException(code);
	case POCO_ECONNREFUSED:
		throw ConnectionRefusedException(arg, code);
	case POCO_EHOSTDOWN:
		throw NetException("Host is down", arg, code);
	case POCO_EHOSTUNREACH:
		throw NetException("No route to host", arg, code);
#if defined(POCO_OS_FAMILY_UNIX)
	case EPIPE:
		throw IOException("Broken pipe", code);
	case EBADF:
		throw IOException("Bad socket descriptor", code);
	case ENOENT:
		throw IOException("Not found", arg, code);
#endif
	default:
		throw IOException(NumberFormatter::format(code), arg, code);
	}
}


} } // namespace Poco::Net
