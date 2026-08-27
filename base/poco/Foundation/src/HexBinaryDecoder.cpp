//
// HexBinaryDecoder.cpp
//
// Library: Foundation
// Package: Streams
// Module:  HexBinary
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/HexBinaryDecoder.h"
#include "Poco/Exception.h"


namespace Poco {


HexBinaryDecoderBuf::HexBinaryDecoderBuf(std::istream& istr): 
	_buf(*istr.rdbuf())
{
}


HexBinaryDecoderBuf::~HexBinaryDecoderBuf()
{
}


int HexBinaryDecoderBuf::readFromDevice()
{
	int c;
	int n;
	if ((n = readOne()) == -1) return -1;
	if (n >= '0' && n <= '9')
		c = n - '0';
	else if (n >= 'A' && n <= 'F')
		c = n - 'A' + 10;
	else if (n >= 'a' && n <= 'f')
		c = n - 'a' + 10;
	else throw DataFormatException();
	c <<= 4;
	if ((n = readOne()) == -1) throw DataFormatException();
	if (n >= '0' && n <= '9')
		c |= n - '0';
	else if (n >= 'A' && n <= 'F')
		c |= n - 'A' + 10;
	else if (n >= 'a' && n <= 'f')
		c |= n - 'a' + 10;
	else throw DataFormatException();
	return c;
}


int HexBinaryDecoderBuf::readOne()
{
	int ch = _buf.sbumpc();
	while (ch == ' ' || ch == '\r' || ch == '\t' || ch == '\n')
		ch = _buf.sbumpc();
	return ch;
}


HexBinaryDecoderIOS::HexBinaryDecoderIOS(std::istream& istr): _buf(istr)
{
	poco_ios_init(&_buf);
}


HexBinaryDecoderIOS::~HexBinaryDecoderIOS()
{
}


HexBinaryDecoderBuf* HexBinaryDecoderIOS::rdbuf()
{
	return &_buf;
}


HexBinaryDecoder::HexBinaryDecoder(std::istream& istr): HexBinaryDecoderIOS(istr), std::istream(&_buf)
{
}


HexBinaryDecoder::~HexBinaryDecoder()
{
}


} // namespace Poco
