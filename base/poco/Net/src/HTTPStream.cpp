//
// HTTPStream.cpp
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
	HTTPBasicStreamBuf(HTTP_DEFAULT_BUFFER_SIZE, mode),
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
		if (!_session.getKeepAlive())
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

HTTPInputStream::HTTPInputStream(HTTPSession& session):
	HTTPIOS(session, std::ios::in),
	std::istream(&_buf)
{
	poco_ios_init(&_buf);
}


HTTPInputStream::~HTTPInputStream()
{
}


//
// HTTPOutputStream
//


HTTPOutputStream::HTTPOutputStream(HTTPSession& session):
	HTTPIOS(session, std::ios::out),
	std::ostream(&_buf)
{
	poco_ios_init(&_buf);
}


HTTPOutputStream::~HTTPOutputStream()
{
}

} } // namespace Poco::Net
