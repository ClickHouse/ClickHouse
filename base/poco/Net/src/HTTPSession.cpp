//
// HTTPSession.cpp
//
// Library: Net
// Package: HTTP
// Module:  HTTPSession
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/HTTPSession.h"
#include "Poco/Net/NetException.h"
#include "Poco/Net/HTTPBasicStreamBuf.h"
#include <cstring>


using Poco::TimeoutException;


namespace Poco {
namespace Net {


HTTPSession::HTTPSession():
	_pBuffer(0),
	_pCurrent(0),
	_pEnd(0),
	_keepAlive(false),
	_connectionTimeout(HTTP_DEFAULT_CONNECTION_TIMEOUT),
	_receiveTimeout(HTTP_DEFAULT_TIMEOUT),
	_sendTimeout(HTTP_DEFAULT_TIMEOUT),
	_pException(0)
{
}


HTTPSession::HTTPSession(const StreamSocket& socket):
	_socket(socket),
	_pBuffer(0),
	_pCurrent(0),
	_pEnd(0),
	_keepAlive(false),
	_connectionTimeout(HTTP_DEFAULT_CONNECTION_TIMEOUT),
	_receiveTimeout(HTTP_DEFAULT_TIMEOUT),
	_sendTimeout(HTTP_DEFAULT_TIMEOUT),
	_pException(0)
{
}


HTTPSession::HTTPSession(const StreamSocket& socket, bool keepAlive):
	_socket(socket),
	_pBuffer(0),
	_pCurrent(0),
	_pEnd(0),
	_keepAlive(keepAlive),
	_connectionTimeout(HTTP_DEFAULT_CONNECTION_TIMEOUT),
	_receiveTimeout(HTTP_DEFAULT_TIMEOUT),
	_sendTimeout(HTTP_DEFAULT_TIMEOUT),
	_pException(0)
{
}


HTTPSession::~HTTPSession()
{
	try
	{
		close();
	}
	catch (...)
	{
	}
	delete _pException;
}


void HTTPSession::setKeepAlive(bool keepAlive)
{
	_keepAlive = keepAlive;
}


void HTTPSession::setTimeout(const Poco::Timespan& timeout)
{
	setTimeout(timeout, timeout, timeout);
}


void HTTPSession::setTimeout(const Poco::Timespan& connectionTimeout, const Poco::Timespan& sendTimeout, const Poco::Timespan& receiveTimeout)
{
     try
     {
         _connectionTimeout = connectionTimeout;

         if (_sendTimeout.totalMicroseconds() != sendTimeout.totalMicroseconds()) {
             _sendTimeout = sendTimeout;

             if (connected())
                 _socket.setSendTimeout(_sendTimeout);
         }

         if (_receiveTimeout.totalMicroseconds() != receiveTimeout.totalMicroseconds()) {
             _receiveTimeout = receiveTimeout;

             if (connected())
                 _socket.setReceiveTimeout(_receiveTimeout);
         }
     }
     catch (NetException &)
     {
#ifndef NDEBUG
         throw;
#else
         // mute exceptions in release
         // just in case when changing settings on socket is not allowed
         // however it should be OK for timeouts
#endif
     }
}


int HTTPSession::get()
{
	if (_pCurrent == _pEnd)
		refill();

	if (_pCurrent < _pEnd)
		return *_pCurrent++;
	else
		return std::char_traits<char>::eof();
}


int HTTPSession::peek()
{
	if (_pCurrent == _pEnd)
		refill();

	if (_pCurrent < _pEnd)
		return *_pCurrent;
	else
		return std::char_traits<char>::eof();
}


int HTTPSession::read(char* buffer, std::streamsize length)
{
	if (_pCurrent < _pEnd)
	{
		int n = (int) (_pEnd - _pCurrent);
		if (n > length) n = (int) length;
		std::memcpy(buffer, _pCurrent, n);
		_pCurrent += n;
		return n;
	}
	else return receive(buffer, (int) length);
}


int HTTPSession::write(const char* buffer, std::streamsize length)
{
	try
	{
		if (_sendDataHooks)
			_sendDataHooks->atStart((int) length);
		int result = _socket.sendBytes(buffer, (int) length);
		if (_sendDataHooks)
			_sendDataHooks->atFinish(result);
		return result;
	}
	catch (Poco::Exception& exc)
	{
		if (_sendDataHooks)
			_sendDataHooks->atFail();
		setException(exc);
		throw;
	}
}


int HTTPSession::receive(char* buffer, int length)
{
	try
	{
		if (_receiveDataHooks)
			_receiveDataHooks->atStart(length);
		int result = _socket.receiveBytes(buffer, length);
		if (_receiveDataHooks)
			_receiveDataHooks->atFinish(result);
		return result;
	}
	catch (Poco::Exception& exc)
	{
		if (_receiveDataHooks)
			_receiveDataHooks->atFail();
		setException(exc);
		throw;
	}
}


void HTTPSession::refill()
{
	if (!_pBuffer)
	{
		_pBuffer = std::make_unique<char[]>(HTTP_DEFAULT_BUFFER_SIZE);
	}
	_pCurrent = _pEnd = _pBuffer.get();
	int n = receive(_pBuffer.get(), HTTP_DEFAULT_BUFFER_SIZE);
	_pEnd += n;
}


bool HTTPSession::connected() const
{
	return _socket.impl()->initialized();
}


void HTTPSession::connect(const SocketAddress& address)
{
	_socket.connect(address, _connectionTimeout);
	_socket.setReceiveTimeout(_receiveTimeout);
	_socket.setSendTimeout(_sendTimeout);
	_socket.setNoDelay(true);
	// There may be leftover data from a previous (failed) request in the buffer,
	// so we clear it.
	_pCurrent = _pEnd = _pBuffer.get();
}


void HTTPSession::abort()
{
	_socket.shutdown();
	close();
}


void HTTPSession::close()
{
	_socket.close();
}


void HTTPSession::setException(const Poco::Exception& exc)
{
	delete _pException;
	_pException = exc.clone();
}


void HTTPSession::clearException()
{
	delete _pException;
	_pException = 0;
}


StreamSocket HTTPSession::detachSocket()
{
	StreamSocket oldSocket(_socket);
	StreamSocket newSocket;
	_socket = newSocket;
	return oldSocket;
}


void HTTPSession::attachSocket(const StreamSocket& socket)
{
	_socket = socket;
}


void HTTPSession::attachSessionData(const Poco::Any& data)
{
	_data = data;
}


void HTTPSession::drainBuffer(Poco::Buffer<char>& buffer)
{
	buffer.assign(_pCurrent, static_cast<std::size_t>(_pEnd - _pCurrent));
	_pCurrent = _pEnd;
}


} } // namespace Poco::Net
