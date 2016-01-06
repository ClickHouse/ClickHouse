//
// HTTPSession.cpp
//
// $Id: //poco/1.4/Net/src/HTTPSession.cpp#2 $
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
#include "Poco/Net/HTTPBufferAllocator.h"
#include "Poco/Net/NetException.h"
#include <cstring>


using Poco::TimeoutException;


namespace Poco {
namespace Net {


HTTPSession::HTTPSession():
	_pBuffer(0),
	_pCurrent(0),
	_pEnd(0),
	_keepAlive(false),
	_timeout(HTTP_DEFAULT_TIMEOUT),
	_pException(0)
{
}


HTTPSession::HTTPSession(const StreamSocket& socket):
	_socket(socket),
	_pBuffer(0),
	_pCurrent(0),
	_pEnd(0),
	_keepAlive(false),
	_timeout(HTTP_DEFAULT_TIMEOUT),
	_pException(0)
{
}


HTTPSession::HTTPSession(const StreamSocket& socket, bool keepAlive):
	_socket(socket),
	_pBuffer(0),
	_pCurrent(0),
	_pEnd(0),
	_keepAlive(keepAlive),
	_timeout(HTTP_DEFAULT_TIMEOUT),
	_pException(0)
{
}


HTTPSession::~HTTPSession()
{
	try
	{
		if (_pBuffer) HTTPBufferAllocator::deallocate(_pBuffer, HTTPBufferAllocator::BUFFER_SIZE);
	}
	catch (...)
	{
		poco_unexpected();
	}
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
	_timeout = timeout;
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
		return _socket.sendBytes(buffer, (int) length);
	}
	catch (Poco::Exception& exc)
	{
		setException(exc);
		throw;
	}
}


int HTTPSession::receive(char* buffer, int length)
{
	try
	{
		return _socket.receiveBytes(buffer, length);
	}
	catch (Poco::Exception& exc)
	{
		setException(exc);
		throw;
	}
}


void HTTPSession::refill()
{
	if (!_pBuffer)
	{
		_pBuffer = HTTPBufferAllocator::allocate(HTTPBufferAllocator::BUFFER_SIZE);
	}
	_pCurrent = _pEnd = _pBuffer;
	int n = receive(_pBuffer, HTTPBufferAllocator::BUFFER_SIZE);
	_pEnd += n;
}


bool HTTPSession::connected() const
{
	return _socket.impl()->initialized();
}


void HTTPSession::connect(const SocketAddress& address)
{
	_socket.connect(address, _timeout);
	_socket.setReceiveTimeout(_timeout);
	_socket.setNoDelay(true);
	// There may be leftover data from a previous (failed) request in the buffer,
	// so we clear it.
	_pCurrent = _pEnd = _pBuffer;
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


} } // namespace Poco::Net
