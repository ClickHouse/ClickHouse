//
// Base32Encoder.cpp
//
// $Id: //poco/1.4/Foundation/src/Base32Encoder.cpp#2 $
//
// Library: Foundation
// Package: Streams
// Module:  Base32
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Base32Encoder.h"


namespace Poco {


const unsigned char Base32EncoderBuf::OUT_ENCODING[32] =
{
	'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
	'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
	'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
	'Y', 'Z', '2', '3', '4', '5', '6', '7',
};


Base32EncoderBuf::Base32EncoderBuf(std::ostream& ostr, bool padding): 
	_groupLength(0),
	_buf(*ostr.rdbuf()),
	_doPadding(padding)
{
}


Base32EncoderBuf::~Base32EncoderBuf()
{
	try
	{
		close();
	}
	catch (...)
	{
	}
}



int Base32EncoderBuf::writeToDevice(char c)
{
	static const int eof = std::char_traits<char>::eof();

	_group[_groupLength++] = (unsigned char) c;
	if (_groupLength == 5)
	{
		unsigned char idx;
		idx = _group[0] >> 3;
		if (_buf.sputc(OUT_ENCODING[idx]) == eof) return eof;
		idx = ((_group[0] & 0x07) << 2) | (_group[1] >> 6);
		if (_buf.sputc(OUT_ENCODING[idx]) == eof) return eof;
		idx = ((_group[1] & 0x3E) >> 1);
		if (_buf.sputc(OUT_ENCODING[idx]) == eof) return eof;
		idx = ((_group[1] & 0x01) << 4) | (_group[2] >> 4);
		if (_buf.sputc(OUT_ENCODING[idx]) == eof) return eof;
		idx = ((_group[2] & 0x0F) << 1) | (_group[3] >> 7);
		if (_buf.sputc(OUT_ENCODING[idx]) == eof) return eof;
		idx = ((_group[3] & 0x7C) >> 2);
		if (_buf.sputc(OUT_ENCODING[idx]) == eof) return eof;
		idx = ((_group[3] & 0x03) << 3) | (_group[4] >> 5);
		if (_buf.sputc(OUT_ENCODING[idx]) == eof) return eof;
		idx = (_group[4] & 0x1F);
		if (_buf.sputc(OUT_ENCODING[idx]) == eof) return eof;
		_groupLength = 0;
	}
	return charToInt(c);
}


int Base32EncoderBuf::close()
{
	static const int eof = std::char_traits<char>::eof();

	if (sync() == eof) return eof;
	if (_groupLength == 1)
	{
		_group[1] = 0;
		unsigned char idx;
		idx = _group[0] >> 3;
		if (_buf.sputc(OUT_ENCODING[idx]) == eof) return eof;
		idx = ((_group[0] & 0x07) << 2);
		if (_buf.sputc(OUT_ENCODING[idx]) == eof) return eof;
		if (_doPadding) {
			if (_buf.sputc('=') == eof) return eof;
			if (_buf.sputc('=') == eof) return eof;
			if (_buf.sputc('=') == eof) return eof;
			if (_buf.sputc('=') == eof) return eof;
			if (_buf.sputc('=') == eof) return eof;
			if (_buf.sputc('=') == eof) return eof;
		}
	}
	else if (_groupLength == 2)
	{
		_group[2] = 0;
		unsigned char idx;
		idx = _group[0] >> 3;
		if (_buf.sputc(OUT_ENCODING[idx]) == eof) return eof;
		idx = ((_group[0] & 0x07) << 2) | (_group[1] >> 6);
		if (_buf.sputc(OUT_ENCODING[idx]) == eof) return eof;
		idx = ((_group[1] & 0x3E) >> 1);
		if (_buf.sputc(OUT_ENCODING[idx]) == eof) return eof;
		idx = ((_group[1] & 0x01) << 4);
		if (_buf.sputc(OUT_ENCODING[idx]) == eof) return eof;
		if (_doPadding) {
			if (_buf.sputc('=') == eof) return eof;
			if (_buf.sputc('=') == eof) return eof;
			if (_buf.sputc('=') == eof) return eof;
			if (_buf.sputc('=') == eof) return eof;
		}
	}
	else if (_groupLength == 3)
	{
		_group[3] = 0;
		unsigned char idx;
		idx = _group[0] >> 3;
		if (_buf.sputc(OUT_ENCODING[idx]) == eof) return eof;
		idx = ((_group[0] & 0x07) << 2) | (_group[1] >> 6);
		if (_buf.sputc(OUT_ENCODING[idx]) == eof) return eof;
		idx = ((_group[1] & 0x3E) >> 1);
		if (_buf.sputc(OUT_ENCODING[idx]) == eof) return eof;
		idx = ((_group[1] & 0x01) << 4) | (_group[2] >> 4);
		if (_buf.sputc(OUT_ENCODING[idx]) == eof) return eof;
		idx = ((_group[2] & 0x0F) << 1);
		if (_buf.sputc(OUT_ENCODING[idx]) == eof) return eof;
		if (_doPadding) {
			if (_buf.sputc('=') == eof) return eof;
			if (_buf.sputc('=') == eof) return eof;
			if (_buf.sputc('=') == eof) return eof;
		}
	}
	else if (_groupLength == 4)
	{
		_group[4] = 0;
		unsigned char idx;
		idx = _group[0] >> 3;
		if (_buf.sputc(OUT_ENCODING[idx]) == eof) return eof;
		idx = ((_group[0] & 0x07) << 2) | (_group[1] >> 6);
		if (_buf.sputc(OUT_ENCODING[idx]) == eof) return eof;
		idx = ((_group[1] & 0x3E) >> 1);
		if (_buf.sputc(OUT_ENCODING[idx]) == eof) return eof;
		idx = ((_group[1] & 0x01) << 4) | (_group[2] >> 4);
		if (_buf.sputc(OUT_ENCODING[idx]) == eof) return eof;
		idx = ((_group[2] & 0x0F) << 1) | (_group[3] >> 7);
		if (_buf.sputc(OUT_ENCODING[idx]) == eof) return eof;
		idx = ((_group[3] & 0x7C) >> 2);
		if (_buf.sputc(OUT_ENCODING[idx]) == eof) return eof;
		idx = ((_group[3] & 0x03) << 3);
		if (_buf.sputc(OUT_ENCODING[idx]) == eof) return eof;
		if (_doPadding && _buf.sputc('=') == eof) return eof;
	}
	_groupLength = 0;
	return _buf.pubsync();
}


Base32EncoderIOS::Base32EncoderIOS(std::ostream& ostr, bool padding):
	_buf(ostr, padding)
{
	poco_ios_init(&_buf);
}


Base32EncoderIOS::~Base32EncoderIOS()
{
}


int Base32EncoderIOS::close()
{
	return _buf.close();
}


Base32EncoderBuf* Base32EncoderIOS::rdbuf()
{
	return &_buf;
}


Base32Encoder::Base32Encoder(std::ostream& ostr, bool padding):
	Base32EncoderIOS(ostr, padding), std::ostream(&_buf)
{
}


Base32Encoder::~Base32Encoder()
{
}


} // namespace Poco
