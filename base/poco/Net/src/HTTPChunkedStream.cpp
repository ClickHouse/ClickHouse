//
// HTTPChunkedStream.cpp
//
// Library: Net
// Package: HTTP
// Module:  HTTPChunkedStream
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/HTTPChunkedStream.h"
#include "Poco/Net/HTTPSession.h"
#include "Poco/Net/NetException.h"
#include "Poco/NumberFormatter.h"
#include "Poco/NumberParser.h"
#include "Poco/Ascii.h"


using Poco::NumberFormatter;
using Poco::NumberParser;


namespace Poco {
namespace Net {


//
// HTTPChunkedStreamBuf
//


HTTPChunkedStreamBuf::HTTPChunkedStreamBuf(HTTPSession& session, openmode mode):
	HTTPBasicStreamBuf(HTTP_DEFAULT_BUFFER_SIZE, mode),
	_session(session),
	_mode(mode),
	_chunk(0)
{
}


HTTPChunkedStreamBuf::~HTTPChunkedStreamBuf()
{
}


void HTTPChunkedStreamBuf::close()
{
	if (_mode & std::ios::out && _chunk != std::char_traits<char>::eof())
	{
		sync();
		_session.write("0\r\n\r\n", 5);

        _chunk = std::char_traits<char>::eof();
	}
}

static inline int assertNonEOF(int c)
{
	if (c == std::char_traits<char>::eof())
		throw MessageException("Unexpected EOF");
    return c;
}

static inline bool isCRLF(char c1, char c2)
{
	return c1 == '\r' && c2 == '\n';
}

unsigned int HTTPChunkedStreamBuf::parseChunkLen()
{
	const size_t maxLineLength = 4096;
	std::string line;
	while (line.size() < maxLineLength)
	{
		int c = assertNonEOF(_session.get());
		line += static_cast<char>(c);
		if (c == '\n') break;
	}

	const size_t n = line.size();
	if (n >= 2 && isCRLF(line[n-2], line[n-1]))
		line.resize(n-2);
	else
		throw MessageException("Malformed chunked encoding");

	if (size_t pos = line.find(';'); pos != std::string::npos)
		line.resize(pos);

	unsigned chunkLen;
	if (NumberParser::tryParseHex(line, chunkLen))
		return chunkLen;
	else
		throw MessageException("Invalid chunk length");
}

void HTTPChunkedStreamBuf::skipCRLF()
{
	int c1 = assertNonEOF(_session.get());
	int c2 = assertNonEOF(_session.get());
	if (!isCRLF(c1, c2))
		throw MessageException("Malformed chunked encoding");
}

int HTTPChunkedStreamBuf::readFromDevice(char* buffer, std::streamsize length)
{
	static const int eof = std::char_traits<char>::eof();
	if (_chunk == eof)
		return 0;

	if (_chunk == 0)
	{
		_chunk = parseChunkLen();
	}

	if (_chunk > 0)
	{
		if (length > _chunk) length = _chunk;
		int n = _session.read(buffer, length);
		if (n > 0)
			_chunk -= n;
		else
			throw MessageException("Unexpected EOF");

		if (_chunk == 0) skipCRLF();
		return n;
	}
	else 
	{
		skipCRLF();
		_chunk = eof;
		return 0;
	}
}


int HTTPChunkedStreamBuf::writeToDevice(const char* buffer, std::streamsize length)
{
	_chunkBuffer.clear();
	NumberFormatter::appendHex(_chunkBuffer, length);
	_chunkBuffer.append("\r\n", 2);
	_chunkBuffer.append(buffer, static_cast<std::string::size_type>(length));
	_chunkBuffer.append("\r\n", 2);
	_session.write(_chunkBuffer.data(), static_cast<std::streamsize>(_chunkBuffer.size()));
	return static_cast<int>(length);
}


//
// HTTPChunkedIOS
//


HTTPChunkedIOS::HTTPChunkedIOS(HTTPSession& session, HTTPChunkedStreamBuf::openmode mode):
	_buf(session, mode)
{
	poco_ios_init(&_buf);
}


HTTPChunkedIOS::~HTTPChunkedIOS()
{
	try
	{
		_buf.close();
	}
	catch (...)
	{
	}
}


HTTPChunkedStreamBuf* HTTPChunkedIOS::rdbuf()
{
	return &_buf;
}


//
// HTTPChunkedInputStream
//

HTTPChunkedInputStream::HTTPChunkedInputStream(HTTPSession& session):
	HTTPChunkedIOS(session, std::ios::in),
	std::istream(&_buf)
{
}


HTTPChunkedInputStream::~HTTPChunkedInputStream()
{
}

//
// HTTPChunkedOutputStream
//

HTTPChunkedOutputStream::HTTPChunkedOutputStream(HTTPSession& session):
	HTTPChunkedIOS(session, std::ios::out),
	std::ostream(&_buf)
{
}


HTTPChunkedOutputStream::~HTTPChunkedOutputStream()
{
}

} } // namespace Poco::Net
