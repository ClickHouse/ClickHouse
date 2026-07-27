//
// QuotedPrintableEncoder.cpp
//
// Library: Net
// Package: Messages
// Module:  QuotedPrintableEncoder
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/QuotedPrintableEncoder.h"
#include "Poco/NumberFormatter.h"


using Poco::UnbufferedStreamBuf;
using Poco::NumberFormatter;


namespace Poco {
namespace Net {


QuotedPrintableEncoderBuf::QuotedPrintableEncoderBuf(std::ostream& ostr): 
	_pending(-1),
	_lineLength(0),
	_ostr(ostr)
{
}


QuotedPrintableEncoderBuf::~QuotedPrintableEncoderBuf()
{
	try
	{
		close();
	}
	catch (...)
	{
	}
}


int QuotedPrintableEncoderBuf::writeToDevice(char c)
{
	if (_pending != -1)
	{
		if (_pending == '\r' && c == '\n')
			writeRaw((char) _pending);
		else if (c == '\r' || c == '\n')
			writeEncoded((char) _pending);
		else
			writeRaw((char) _pending);
		_pending = -1;
	}
	if (c == '\t' || c == ' ')
	{
		_pending = charToInt(c);
		return _pending;
	}
	else if (c == '\r' || c == '\n' || (c > 32 && c < 127 && c != '='))
	{
		writeRaw(c);
	}
	else
	{
		writeEncoded(c);
	}
	return charToInt(c);
}


void QuotedPrintableEncoderBuf::writeEncoded(char c)
{
	if (_lineLength >= 73)
	{
		_ostr << "=\r\n";
		_lineLength = 3;
	}
	else _lineLength += 3;
	_ostr << '=' << NumberFormatter::formatHex((unsigned) charToInt(c), 2);
}


void QuotedPrintableEncoderBuf::writeRaw(char c)
{
	if (c == '\r' || c == '\n')
	{
		_ostr.put(c);
		_lineLength = 0;
	}
	else if (_lineLength < 75)
	{
		_ostr.put(c);
		++_lineLength;
	}
	else
	{
		_ostr << "=\r\n" << c;
		_lineLength = 1;
	}
}


int QuotedPrintableEncoderBuf::close()
{
	sync();
	return _ostr ? 0 : -1;
}


QuotedPrintableEncoderIOS::QuotedPrintableEncoderIOS(std::ostream& ostr): _buf(ostr)
{
	poco_ios_init(&_buf);
}


QuotedPrintableEncoderIOS::~QuotedPrintableEncoderIOS()
{
}


int QuotedPrintableEncoderIOS::close()
{
	return _buf.close();
}


QuotedPrintableEncoderBuf* QuotedPrintableEncoderIOS::rdbuf()
{
	return &_buf;
}


QuotedPrintableEncoder::QuotedPrintableEncoder(std::ostream& ostr): 
	QuotedPrintableEncoderIOS(ostr), 
	std::ostream(&_buf)
{
}


QuotedPrintableEncoder::~QuotedPrintableEncoder()
{
}


} } // namespace Poco::Net
