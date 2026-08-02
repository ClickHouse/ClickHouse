//
// Base64Encoder.cpp
//
// Library: Foundation
// Package: Streams
// Module:  Base64
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Base64Encoder.h"


namespace Poco {


const unsigned char Base64EncoderBuf::OUT_ENCODING[64] =
{
	'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
	'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
	'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
	'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f',
	'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
	'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
	'w', 'x', 'y', 'z', '0', '1', '2', '3',
	'4', '5', '6', '7', '8', '9', '+', '/'
};


const unsigned char Base64EncoderBuf::OUT_ENCODING_URL[64] =
{
	'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
	'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
	'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
	'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f',
	'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
	'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
	'w', 'x', 'y', 'z', '0', '1', '2', '3',
	'4', '5', '6', '7', '8', '9', '-', '_'
};


Base64EncoderBuf::Base64EncoderBuf(std::ostream& ostr, int options):
	_options(options),
	_groupLength(0),
	_pos(0),
	_lineLength((options & BASE64_URL_ENCODING) ? 0 : 72),
	_buf(*ostr.rdbuf()),
	_pOutEncoding((options & BASE64_URL_ENCODING) ? OUT_ENCODING_URL : OUT_ENCODING)
{
}


Base64EncoderBuf::~Base64EncoderBuf()
{
	try
	{
		close();
	}
	catch (...)
	{
	}
}


void Base64EncoderBuf::setLineLength(int lineLength)
{
	_lineLength = lineLength;
}


int Base64EncoderBuf::getLineLength() const
{
	return _lineLength;
}


int Base64EncoderBuf::writeToDevice(char c)
{
	static const int eof = std::char_traits<char>::eof();

	_group[_groupLength++] = (unsigned char) c;
	if (_groupLength == 3)
	{
		unsigned char idx;
		idx = _group[0] >> 2;
		if (_buf.sputc(_pOutEncoding[idx]) == eof) return eof;
		idx = ((_group[0] & 0x03) << 4) | (_group[1] >> 4);
		if (_buf.sputc(_pOutEncoding[idx]) == eof) return eof;
		idx = ((_group[1] & 0x0F) << 2) | (_group[2] >> 6);
		if (_buf.sputc(_pOutEncoding[idx]) == eof) return eof;
		idx = _group[2] & 0x3F;
		if (_buf.sputc(_pOutEncoding[idx]) == eof) return eof;
		_pos += 4;
		if (_lineLength > 0 && _pos >= _lineLength)
		{
			if (_buf.sputc('\r') == eof) return eof;
			if (_buf.sputc('\n') == eof) return eof;
			_pos = 0;
		}
		_groupLength = 0;
	}
	return charToInt(c);
}


int Base64EncoderBuf::close()
{
	static const int eof = std::char_traits<char>::eof();

	if (sync() == eof) return eof;
	if (_groupLength == 1)
	{
		_group[1] = 0;
		unsigned char idx;
		idx = _group[0] >> 2;
		if (_buf.sputc(_pOutEncoding[idx]) == eof) return eof;
		idx = ((_group[0] & 0x03) << 4) | (_group[1] >> 4);
		if (_buf.sputc(_pOutEncoding[idx]) == eof) return eof;
		if (!(_options & BASE64_NO_PADDING))
		{
			if (_buf.sputc('=') == eof) return eof;
			if (_buf.sputc('=') == eof) return eof;
		}
	}
	else if (_groupLength == 2)
	{
		_group[2] = 0;
		unsigned char idx;
		idx = _group[0] >> 2;
		if (_buf.sputc(_pOutEncoding[idx]) == eof) return eof;
		idx = ((_group[0] & 0x03) << 4) | (_group[1] >> 4);
		if (_buf.sputc(_pOutEncoding[idx]) == eof) return eof;
		idx = ((_group[1] & 0x0F) << 2) | (_group[2] >> 6);
		if (_buf.sputc(_pOutEncoding[idx]) == eof) return eof;
		if (!(_options & BASE64_NO_PADDING))
		{
			if (_buf.sputc('=') == eof) return eof;
		}
	}
	_groupLength = 0;
	return _buf.pubsync();
}


Base64EncoderIOS::Base64EncoderIOS(std::ostream& ostr, int options): _buf(ostr, options)
{
	poco_ios_init(&_buf);
}


Base64EncoderIOS::~Base64EncoderIOS()
{
}


int Base64EncoderIOS::close()
{
	return _buf.close();
}


Base64EncoderBuf* Base64EncoderIOS::rdbuf()
{
	return &_buf;
}


Base64Encoder::Base64Encoder(std::ostream& ostr, int options): Base64EncoderIOS(ostr, options), std::ostream(&_buf)
{
}


Base64Encoder::~Base64Encoder()
{
}


} // namespace Poco
