//
// HTTPFixedLengthStream.cpp
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
#include "Poco/Net/NetException.h"


using Poco::BufferedStreamBuf;


namespace Poco {
namespace Net {


//
// HTTPFixedLengthStreamBuf
//


HTTPFixedLengthStreamBuf::HTTPFixedLengthStreamBuf(HTTPSession& session, ContentLength length, openmode mode):
	HTTPBasicStreamBuf(HTTP_DEFAULT_BUFFER_SIZE, mode),
	_session(session),
	_length(length),
	_count(0)
{
}


HTTPFixedLengthStreamBuf::~HTTPFixedLengthStreamBuf()
{
}


bool HTTPFixedLengthStreamBuf::isComplete() const
{
    return _count == _length;
}


int HTTPFixedLengthStreamBuf::readFromDevice(char* buffer, std::streamsize length)
{
	int n = 0;
	if (_count < _length)
	{
		if (_count + length > _length)
			length = static_cast<std::streamsize>(_length - _count);
		n = _session.read(buffer, length);
		if (n > 0)
			_count += n;
		else
			throw MessageException("Unexpected EOF");
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


HTTPFixedLengthInputStream::HTTPFixedLengthInputStream(HTTPSession& session, HTTPFixedLengthStreamBuf::ContentLength length):
	HTTPFixedLengthIOS(session, length, std::ios::in),
	std::istream(&_buf)
{
}


HTTPFixedLengthInputStream::~HTTPFixedLengthInputStream()
{
}


//
// HTTPFixedLengthOutputStream
//

HTTPFixedLengthOutputStream::HTTPFixedLengthOutputStream(HTTPSession& session, HTTPFixedLengthStreamBuf::ContentLength length):
	HTTPFixedLengthIOS(session, length, std::ios::out),
	std::ostream(&_buf)
{
}


HTTPFixedLengthOutputStream::~HTTPFixedLengthOutputStream()
{
}


} } // namespace Poco::Net
