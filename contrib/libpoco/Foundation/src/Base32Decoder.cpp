//
// Base32Decoder.cpp
//
// $Id: //poco/1.4/Foundation/src/Base32Decoder.cpp#2 $
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


#include "Poco/Base32Decoder.h"
#include "Poco/Base32Encoder.h"
#include "Poco/Exception.h"
#include "Poco/Mutex.h"
#include <cstring>


namespace Poco {


unsigned char Base32DecoderBuf::IN_ENCODING[256];
bool Base32DecoderBuf::IN_ENCODING_INIT = false;


namespace
{
	static FastMutex mutex;
}


Base32DecoderBuf::Base32DecoderBuf(std::istream& istr): 
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
		for (unsigned i = 0; i < sizeof(Base32EncoderBuf::OUT_ENCODING); i++)
		{
			IN_ENCODING[Base32EncoderBuf::OUT_ENCODING[i]] = i;
		}
		IN_ENCODING[static_cast<unsigned char>('=')] = '\0';
		IN_ENCODING_INIT = true;
	}
}


Base32DecoderBuf::~Base32DecoderBuf()
{
}


int Base32DecoderBuf::readFromDevice()
{
	if (_groupIndex < _groupLength) 
	{
		return _group[_groupIndex++];
	}
	else
	{
		unsigned char buffer[8];
		std::memset(buffer, '=', sizeof(buffer));
		int c;

		// per RFC-4648, Section 6, permissible block lengths are:
		// 2, 4, 5, 7, and 8 bytes. Any other length is malformed.
		//
		do {
			if ((c = readOne()) == -1) return -1;
			buffer[0] = (unsigned char) c;
			if (IN_ENCODING[buffer[0]] == 0xFF) throw DataFormatException();
			if ((c = readOne()) == -1) throw DataFormatException();
			buffer[1] = (unsigned char) c;
			if (IN_ENCODING[buffer[1]] == 0xFF) throw DataFormatException();
			if ((c = readOne()) == -1) break;
			buffer[2] = (unsigned char) c;
			if (IN_ENCODING[buffer[2]] == 0xFF) throw DataFormatException();
			if ((c = readOne()) == -1) throw DataFormatException();
			buffer[3] = (unsigned char) c;
			if (IN_ENCODING[buffer[3]] == 0xFF) throw DataFormatException();
			if ((c = readOne()) == -1) break;
			buffer[4] = (unsigned char) c;
			if (IN_ENCODING[buffer[4]] == 0xFF) throw DataFormatException();
			if ((c = readOne()) == -1) break;
			buffer[5] = (unsigned char) c;
			if (IN_ENCODING[buffer[5]] == 0xFF) throw DataFormatException();
			if ((c = readOne()) == -1) throw DataFormatException();
			buffer[6] = (unsigned char) c;
			if (IN_ENCODING[buffer[6]] == 0xFF) throw DataFormatException();
			if ((c = readOne()) == -1) break;
			buffer[7] = (unsigned char) c;
			if (IN_ENCODING[buffer[7]] == 0xFF) throw DataFormatException();
		} while (false);

		_group[0] = (IN_ENCODING[buffer[0]] << 3) | (IN_ENCODING[buffer[1]] >> 2);
		_group[1] = ((IN_ENCODING[buffer[1]] & 0x03) << 6) | (IN_ENCODING[buffer[2]] << 1) | (IN_ENCODING[buffer[3]] >> 4);
		_group[2] = ((IN_ENCODING[buffer[3]] & 0x0F) << 4) | (IN_ENCODING[buffer[4]] >> 1);
		_group[3] = ((IN_ENCODING[buffer[4]] & 0x01) << 7) | (IN_ENCODING[buffer[5]] << 2) | (IN_ENCODING[buffer[6]] >> 3);
		_group[4] = ((IN_ENCODING[buffer[6]] & 0x07) << 5) | IN_ENCODING[buffer[7]];

		if (buffer[2] == '=')
			_groupLength = 1;
		else if (buffer[4] == '=') 
			_groupLength = 2;
		else if (buffer[5] == '=') 
			_groupLength = 3;
		else if (buffer[7] == '=') 
			_groupLength = 4;
		else
			_groupLength = 5;
		_groupIndex = 1;
		return _group[0];
	}
}


int Base32DecoderBuf::readOne()
{
	int ch = _buf.sbumpc();
	return ch;
}


Base32DecoderIOS::Base32DecoderIOS(std::istream& istr): _buf(istr)
{
	poco_ios_init(&_buf);
}


Base32DecoderIOS::~Base32DecoderIOS()
{
}


Base32DecoderBuf* Base32DecoderIOS::rdbuf()
{
	return &_buf;
}


Base32Decoder::Base32Decoder(std::istream& istr): Base32DecoderIOS(istr), std::istream(&_buf)
{
}


Base32Decoder::~Base32Decoder()
{
}


} // namespace Poco
