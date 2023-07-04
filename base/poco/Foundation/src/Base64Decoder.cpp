//
// Base64Decoder.cpp
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
unsigned char Base64DecoderBuf::IN_ENCODING_URL[256];
bool Base64DecoderBuf::IN_ENCODING_URL_INIT = false;


namespace
{
	static FastMutex mutex;
}


Base64DecoderBuf::Base64DecoderBuf(std::istream& istr, int options):
	_options(options),
	_groupLength(0),
	_groupIndex(0),
	_buf(*istr.rdbuf()),
	_pInEncoding((options & BASE64_URL_ENCODING) ? IN_ENCODING_URL : IN_ENCODING)
{
	FastMutex::ScopedLock lock(mutex);
	if (options & BASE64_URL_ENCODING)
	{
		if (!IN_ENCODING_URL_INIT)
		{
			for (unsigned i = 0; i < sizeof(IN_ENCODING_URL); i++)
			{
				IN_ENCODING_URL[i] = 0xFF;
			}
			for (unsigned i = 0; i < sizeof(Base64EncoderBuf::OUT_ENCODING_URL); i++)
			{
				IN_ENCODING_URL[Base64EncoderBuf::OUT_ENCODING_URL[i]] = i;
			}
			IN_ENCODING_URL[static_cast<unsigned char>('=')] = '\0';
			IN_ENCODING_URL_INIT = true;
		}
	}
	else
	{
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
		if (_pInEncoding[buffer[0]] == 0xFF) throw DataFormatException();
		if ((c = readOne()) == -1) return -1;
		buffer[1] = (unsigned char) c;
		if (_pInEncoding[buffer[1]] == 0xFF) throw DataFormatException();
		if (_options & BASE64_NO_PADDING)
		{
			if ((c = readOne()) != -1)
				buffer[2] = c;
			else
				buffer[2] = '=';
			if (_pInEncoding[buffer[2]] == 0xFF) throw DataFormatException();
			if ((c = readOne()) != -1)
				buffer[3] = c;
			else
				buffer[3] = '=';
			if (_pInEncoding[buffer[3]] == 0xFF) throw DataFormatException();
		}
		else
		{
			if ((c = readOne()) == -1) throw DataFormatException();
			buffer[2] = c;
			if (_pInEncoding[buffer[2]] == 0xFF) throw DataFormatException();
			if ((c = readOne()) == -1) throw DataFormatException();
			buffer[3] = c;
			if (_pInEncoding[buffer[3]] == 0xFF) throw DataFormatException();
		}

		_group[0] = (_pInEncoding[buffer[0]] << 2) | (_pInEncoding[buffer[1]] >> 4);
		_group[1] = ((_pInEncoding[buffer[1]] & 0x0F) << 4) | (_pInEncoding[buffer[2]] >> 2);
		_group[2] = (_pInEncoding[buffer[2]] << 6) | _pInEncoding[buffer[3]];

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
	if (!(_options & BASE64_URL_ENCODING))
	{
		while (ch == ' ' || ch == '\r' || ch == '\t' || ch == '\n')
			ch = _buf.sbumpc();
	}
	return ch;
}


Base64DecoderIOS::Base64DecoderIOS(std::istream& istr, int options): _buf(istr, options)
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


Base64Decoder::Base64Decoder(std::istream& istr, int options): Base64DecoderIOS(istr, options), std::istream(&_buf)
{
}


Base64Decoder::~Base64Decoder()
{
}


} // namespace Poco
