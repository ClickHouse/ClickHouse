//
// StreamConverter.cpp
//
// $Id: //poco/1.4/Foundation/src/StreamConverter.cpp#1 $
//
// Library: Foundation
// Package: Text
// Module:  StreamConverter
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/StreamConverter.h"
#include "Poco/TextEncoding.h"


namespace Poco {


StreamConverterBuf::StreamConverterBuf(std::istream& istr, const TextEncoding& inEncoding, const TextEncoding& outEncoding, int defaultChar):
	_pIstr(&istr),
	_pOstr(0),
	_inEncoding(inEncoding),
	_outEncoding(outEncoding),
	_defaultChar(defaultChar),
	_sequenceLength(0),
	_pos(0),
	_errors(0)
{
}


StreamConverterBuf::StreamConverterBuf(std::ostream& ostr, const TextEncoding& inEncoding, const TextEncoding& outEncoding, int defaultChar):
	_pIstr(0),
	_pOstr(&ostr),
	_inEncoding(inEncoding),
	_outEncoding(outEncoding),
	_defaultChar(defaultChar),
	_sequenceLength(0),
	_pos(0),
	_errors(0)
{
}


StreamConverterBuf::~StreamConverterBuf()
{
}


int StreamConverterBuf::readFromDevice()
{
	poco_assert_dbg (_pIstr);

	if (_pos < _sequenceLength) return _buffer[_pos++];

	_pos = 0;
	_sequenceLength = 0;
	int c = _pIstr->get();
	if (c == -1) return -1;	

	poco_assert (c < 256);
	int uc;
	_buffer [0] = (unsigned char) c;
	int n = _inEncoding.queryConvert(_buffer, 1);
	int read = 1;

	while (-1 > n)
	{
		poco_assert_dbg(-n <= sizeof(_buffer));
		_pIstr->read((char*) _buffer + read, -n - read);
		read = -n;
		n = _inEncoding.queryConvert(_buffer, -n);
	}

	if (-1 >= n)
	{
		uc = _defaultChar;
		++_errors;
	}
	else
	{
		uc = n;
	}

	_sequenceLength = _outEncoding.convert(uc, _buffer, sizeof(_buffer));
	if (_sequenceLength == 0)
		_sequenceLength = _outEncoding.convert(_defaultChar, _buffer, sizeof(_buffer));
	if (_sequenceLength == 0)
		return -1;
	else
		return _buffer[_pos++];
}


int StreamConverterBuf::writeToDevice(char c)
{
	poco_assert_dbg (_pOstr);

	_buffer[_pos++] = (unsigned char) c;
	if (_sequenceLength == 0 || _sequenceLength == _pos)
	{
		int n = _inEncoding.queryConvert(_buffer, _pos);
		if (-1 <= n)
		{
			int uc = n;
			if (-1 == n)
			{
				++_errors;
				return -1;
			}
			int n = _outEncoding.convert(uc, _buffer, sizeof(_buffer));
			if (n == 0) n = _outEncoding.convert(_defaultChar, _buffer, sizeof(_buffer));
			poco_assert_dbg (n <= sizeof(_buffer));
			_pOstr->write((char*) _buffer, n);
			_sequenceLength = 0;
			_pos = 0;
		}
		else
		{
			_sequenceLength = -n;
		}
	}

	return charToInt(c);
}


int StreamConverterBuf::errors() const
{
	return _errors;
}


StreamConverterIOS::StreamConverterIOS(std::istream& istr, const TextEncoding& inEncoding, const TextEncoding& outEncoding, int defaultChar): 
	_buf(istr, inEncoding, outEncoding, defaultChar)
{
	poco_ios_init(&_buf);
}


StreamConverterIOS::StreamConverterIOS(std::ostream& ostr, const TextEncoding& inEncoding, const TextEncoding& outEncoding, int defaultChar): 
	_buf(ostr, inEncoding, outEncoding, defaultChar)
{
	poco_ios_init(&_buf);
}


StreamConverterIOS::~StreamConverterIOS()
{
}


StreamConverterBuf* StreamConverterIOS::rdbuf()
{
	return &_buf;
}


int StreamConverterIOS::errors() const
{
	return _buf.errors();
}


InputStreamConverter::InputStreamConverter(std::istream& istr, const TextEncoding& inEncoding, const TextEncoding& outEncoding, int defaultChar): 
	StreamConverterIOS(istr, inEncoding, outEncoding, defaultChar),
	std::istream(&_buf)
{
}


InputStreamConverter::~InputStreamConverter()
{
}


OutputStreamConverter::OutputStreamConverter(std::ostream& ostr, const TextEncoding& inEncoding, const TextEncoding& outEncoding, int defaultChar): 
	StreamConverterIOS(ostr, inEncoding, outEncoding, defaultChar),
	std::ostream(&_buf)
{
}


OutputStreamConverter::~OutputStreamConverter()
{
}


} // namespace Poco
