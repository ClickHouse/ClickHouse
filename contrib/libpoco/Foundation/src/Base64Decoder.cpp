//
// Base64Decoder.cpp
//
// $Id: //poco/1.4/Foundation/src/Base64Decoder.cpp#2 $
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


#include "Poco/Base64Decoder.h"
#include "Poco/Base64Encoder.h"
#include "Poco/Exception.h"
#include "Poco/Mutex.h"


namespace Poco {


unsigned char Base64DecoderBuf::IN_ENCODING[256];
bool Base64DecoderBuf::IN_ENCODING_INIT = false;


namespace
{
	static FastMutex mutex;
}


Base64DecoderBuf::Base64DecoderBuf(std::istream& istr): 
	_groupLength(0),
	_groupIndex(0),
	_buf(*istr.rdbuf())
{
	FastMutex::ScopedLock lock(mutex);
	if (!IN_ENCODING_INIT)
	{
		for (unsigned i = 0; i < sizeof(IN_ENCODING); i++)
		{
			IN_ENCODING[i] = 0xFF;
		}
		for (unsigned i = 0; i < sizeof(Base64EncoderBuf::OUT_ENCODING); i++)
		{
			IN_ENCODING[Base64EncoderBuf::OUT_ENCODING[i]] = i;
		}
		IN_ENCODING[static_cast<unsigned char>('=')] = '\0';
		IN_ENCODING_INIT = true;
	}
}


Base64DecoderBuf::~Base64DecoderBuf()
{
}


int Base64DecoderBuf::readFromDevice()
{
	if (_groupIndex < _groupLength) 
	{
		return _group[_groupIndex++];
	}
	else
	{
		unsigned char buffer[4];
		int c;
		if ((c = readOne()) == -1) return -1;
		buffer[0] = (unsigned char) c;
		if (IN_ENCODING[buffer[0]] == 0xFF) throw DataFormatException();
		if ((c = readOne()) == -1) throw DataFormatException();
		buffer[1] = (unsigned char) c;
		if (IN_ENCODING[buffer[1]] == 0xFF) throw DataFormatException();
		if ((c = readOne()) == -1) throw DataFormatException();
		buffer[2] = c;
		if (IN_ENCODING[buffer[2]] == 0xFF) throw DataFormatException();
		if ((c = readOne()) == -1) throw DataFormatException();
		buffer[3] = c;
		if (IN_ENCODING[buffer[3]] == 0xFF) throw DataFormatException();
		
		_group[0] = (IN_ENCODING[buffer[0]] << 2) | (IN_ENCODING[buffer[1]] >> 4);
		_group[1] = ((IN_ENCODING[buffer[1]] & 0x0F) << 4) | (IN_ENCODING[buffer[2]] >> 2);
		_group[2] = (IN_ENCODING[buffer[2]] << 6) | IN_ENCODING[buffer[3]];

		if (buffer[2] == '=')
			_groupLength = 1;
		else if (buffer[3] == '=') 
			_groupLength = 2;
		else
			_groupLength = 3;
		_groupIndex = 1;
		return _group[0];
	}
}


int Base64DecoderBuf::readOne()
{
	int ch = _buf.sbumpc();
	while (ch == ' ' || ch == '\r' || ch == '\t' || ch == '\n')
		ch = _buf.sbumpc();
	return ch;
}


Base64DecoderIOS::Base64DecoderIOS(std::istream& istr): _buf(istr)
{
	poco_ios_init(&_buf);
}


Base64DecoderIOS::~Base64DecoderIOS()
{
}


Base64DecoderBuf* Base64DecoderIOS::rdbuf()
{
	return &_buf;
}


Base64Decoder::Base64Decoder(std::istream& istr): Base64DecoderIOS(istr), std::istream(&_buf)
{
}


Base64Decoder::~Base64Decoder()
{
}


} // namespace Poco
