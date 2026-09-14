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

#include <algorithm>


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


std::size_t HTTPFixedLengthStreamBuf::tryDrainBufferedRemainder(std::size_t max_bytes)
{
    if (_count >= _length)
        return 0;

    const auto remaining = static_cast<std::size_t>(_length - _count);
    if (remaining > max_bytes || remaining > static_cast<std::size_t>(_session.buffered()))
        return 0;

    /// Do not call through the consumer's stream buffer: its memory may have been released or
    /// may still contain data being consumed by an asynchronous reader.
    char scratch[1024];
    std::size_t drained = 0;
    while (drained < remaining)
        drained += readFromDevice(scratch, std::min(sizeof(scratch), remaining - drained));
    return drained;
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
	if (_count + length > _length)
		throw MessageException("Write past Content-Length");

	int n = _session.write(buffer, length);
	if (n > 0) _count += n;
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
	poco_ios_init(&_buf);
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
	poco_ios_init(&_buf);
}


HTTPFixedLengthOutputStream::~HTTPFixedLengthOutputStream()
{
}


} } // namespace Poco::Net
