//
// HTTPFixedLengthStream.cpp
//
// $Id: //poco/1.4/Net/src/HTTPFixedLengthStream.cpp#1 $
//
// Library: Net
// Package: HTTP
// Module:  HTTPFixedLengthStream
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/HTTPFixedLengthStream.h"
#include "Poco/Net/HTTPSession.h"


using Poco::BufferedStreamBuf;


namespace Poco {
namespace Net {


//
// HTTPFixedLengthStreamBuf
//


HTTPFixedLengthStreamBuf::HTTPFixedLengthStreamBuf(HTTPSession& session, ContentLength length, openmode mode):
	HTTPBasicStreamBuf(HTTPBufferAllocator::BUFFER_SIZE, mode),
	_session(session),
	_length(length),
	_count(0)
{
}


HTTPFixedLengthStreamBuf::~HTTPFixedLengthStreamBuf()
{
}


int HTTPFixedLengthStreamBuf::readFromDevice(char* buffer, std::streamsize length)
{
	int n = 0;
	if (_count < _length)
	{
		if (_count + length > _length)
			length = static_cast<std::streamsize>(_length - _count);
		n = _session.read(buffer, length);
		if (n > 0) _count += n;
	}
	return n;
}


int HTTPFixedLengthStreamBuf::writeToDevice(const char* buffer, std::streamsize length)
{
	int n = 0;
	if (_count < _length)
	{
		if (_count + length > _length)
			length = static_cast<std::streamsize>(_length - _count);
		n = _session.write(buffer, length);
		if (n > 0) _count += n;
	}
	return n;
}


//
// HTTPFixedLengthIOS
//


HTTPFixedLengthIOS::HTTPFixedLengthIOS(HTTPSession& session, HTTPFixedLengthStreamBuf::ContentLength length, HTTPFixedLengthStreamBuf::openmode mode):
	_buf(session, length, mode)
{
	poco_ios_init(&_buf);
}


HTTPFixedLengthIOS::~HTTPFixedLengthIOS()
{
	try
	{
		_buf.sync();
	}
	catch (...)
	{
	}
}


HTTPFixedLengthStreamBuf* HTTPFixedLengthIOS::rdbuf()
{
	return &_buf;
}


//
// HTTPFixedLengthInputStream
//


Poco::MemoryPool HTTPFixedLengthInputStream::_pool(sizeof(HTTPFixedLengthInputStream));


HTTPFixedLengthInputStream::HTTPFixedLengthInputStream(HTTPSession& session, HTTPFixedLengthStreamBuf::ContentLength length):
	HTTPFixedLengthIOS(session, length, std::ios::in),
	std::istream(&_buf)
{
}


HTTPFixedLengthInputStream::~HTTPFixedLengthInputStream()
{
}


void* HTTPFixedLengthInputStream::operator new(std::size_t size)
{
	return _pool.get();
}


void HTTPFixedLengthInputStream::operator delete(void* ptr)
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
// HTTPFixedLengthOutputStream
//


Poco::MemoryPool HTTPFixedLengthOutputStream::_pool(sizeof(HTTPFixedLengthOutputStream));


HTTPFixedLengthOutputStream::HTTPFixedLengthOutputStream(HTTPSession& session, HTTPFixedLengthStreamBuf::ContentLength length):
	HTTPFixedLengthIOS(session, length, std::ios::out),
	std::ostream(&_buf)
{
}


HTTPFixedLengthOutputStream::~HTTPFixedLengthOutputStream()
{
}


void* HTTPFixedLengthOutputStream::operator new(std::size_t size)
{
	return _pool.get();
}


void HTTPFixedLengthOutputStream::operator delete(void* ptr)
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
