//
// QuotedPrintableDecoder.cpp
//
// $Id: //poco/1.4/Net/src/QuotedPrintableDecoder.cpp#2 $
//
// Library: Net
// Package: Messages
// Module:  QuotedPrintableDecoder
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/QuotedPrintableDecoder.h"
#include "Poco/NumberParser.h"
#include "Poco/Exception.h"
#include "Poco/Ascii.h"


using Poco::UnbufferedStreamBuf;
using Poco::NumberParser;
using Poco::DataFormatException;


namespace Poco {
namespace Net {


QuotedPrintableDecoderBuf::QuotedPrintableDecoderBuf(std::istream& istr): 
	_buf(*istr.rdbuf())
{
}


QuotedPrintableDecoderBuf::~QuotedPrintableDecoderBuf()
{
}


int QuotedPrintableDecoderBuf::readFromDevice()
{
	int ch = _buf.sbumpc();
	while (ch == '=')
	{
		ch = _buf.sbumpc();
		if (ch == '\r')
		{
			ch = _buf.sbumpc(); // read \n
		}
		else if (Poco::Ascii::isHexDigit(ch))
		{
			std::string hex = "0x";
			hex += (char) ch;
			ch = _buf.sbumpc();
			if (Poco::Ascii::isHexDigit(ch))
			{
				hex += (char) ch;
				return NumberParser::parseHex(hex);
			}
			throw DataFormatException("Incomplete hex number in quoted-printable encoded stream");
		}
		else if (ch != '\n')
		{
			throw DataFormatException("Invalid occurrence of '=' in quoted-printable encoded stream");
		}
		ch = _buf.sbumpc();
	}
	return ch;
}


QuotedPrintableDecoderIOS::QuotedPrintableDecoderIOS(std::istream& istr): _buf(istr)
{
	poco_ios_init(&_buf);
}


QuotedPrintableDecoderIOS::~QuotedPrintableDecoderIOS()
{
}


QuotedPrintableDecoderBuf* QuotedPrintableDecoderIOS::rdbuf()
{
	return &_buf;
}


QuotedPrintableDecoder::QuotedPrintableDecoder(std::istream& istr): 
	QuotedPrintableDecoderIOS(istr), 
	std::istream(&_buf)
{
}


QuotedPrintableDecoder::~QuotedPrintableDecoder()
{
}


} } // namespace Poco::Net
