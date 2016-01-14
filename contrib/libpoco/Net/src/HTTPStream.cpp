//
// HTTPStream.cpp
//
// $Id: //poco/1.4/Net/src/HTTPStream.cpp#1 $
//
// Library: Net
// Package: HTTP
// Module:  HTTPStream
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/HTTPStream.h"
#include "Poco/Net/HTTPSession.h"


namespace Poco {
namespace Net {


//
// HTTPStreamBuf
//


HTTPStreamBuf::HTTPStreamBuf(HTTPSession& session, openmode mode):
	HTTPBasicStreamBuf(HTTPBufferAllocator::BUFFER_SIZE, mode),
	_session(session),
	_mode(mode)
{
}


HTTPStreamBuf::~HTTPStreamBuf()
{
}


void HTTPStreamBuf::close()
{
	if (_mode & std::ios::out)
	{
		sync();
		_session.socket().shutdownSend();
	}
}


int HTTPStreamBuf::readFromDevice(char* buffer, std::streamsize length)
{
	return _session.read(buffer, length);
}


int HTTPStreamBuf::writeToDevice(const char* buffer, std::streamsize length)
{

	return _session.write(buffer, length);
}


//
// HTTPIOS
//


HTTPIOS::HTTPIOS(HTTPSession& session, HTTPStreamBuf::openmode mode):
	_buf(session, mode)
{
	poco_ios_init(&_buf);
}


HTTPIOS::~HTTPIOS()
{
	try
	{
		_buf.close();
	}
	catch (...)
	{
	}
}


HTTPStreamBuf* HTTPIOS::rdbuf()
{
	return &_buf;
}


//
// HTTPInputStream
//


Poco::MemoryPool HTTPInputStream::_pool(sizeof(HTTPInputStream));


HTTPInputStream::HTTPInputStream(HTTPSession& session):
	HTTPIOS(session, std::ios::in),
	std::istream(&_buf)
{
}


HTTPInputStream::~HTTPInputStream()
{
}


void* HTTPInputStream::operator new(std::size_t size)
{
	return _pool.get();
}


void HTTPInputStream::operator delete(void* ptr)
{
	try
	{
		_pool.release(ptr);
	}
	catch (...)
	{
		poco_unexpected();
	}
}


//
// HTTPOutputStream
//


Poco::MemoryPool HTTPOutputStream::_pool(sizeof(HTTPOutputStream));


HTTPOutputStream::HTTPOutputStream(HTTPSession& session):
	HTTPIOS(session, std::ios::out),
	std::ostream(&_buf)
{
}


HTTPOutputStream::~HTTPOutputStream()
{
}


void* HTTPOutputStream::operator new(std::size_t size)
{
	return _pool.get();
}


void HTTPOutputStream::operator delete(void* ptr)
{
	try
	{
		_pool.release(ptr);
	}
	catch (...)
	{
		poco_unexpected();
	}
}


} } // namespace Poco::Net
