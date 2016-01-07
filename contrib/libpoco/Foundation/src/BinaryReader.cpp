//
// BinaryReader.cpp
//
// $Id: //poco/1.4/Foundation/src/BinaryReader.cpp#1 $
//
// Library: Foundation
// Package: Streams
// Module:  BinaryReaderWriter
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/BinaryReader.h"
#include "Poco/ByteOrder.h"
#include "Poco/TextEncoding.h"
#include "Poco/TextConverter.h"
#include <algorithm>


namespace Poco {


BinaryReader::BinaryReader(std::istream& istr, StreamByteOrder byteOrder):
	_istr(istr),
	_pTextConverter(0)
{
#if defined(POCO_ARCH_BIG_ENDIAN)
	_flipBytes = (byteOrder == LITTLE_ENDIAN_BYTE_ORDER);
#else
	_flipBytes = (byteOrder == BIG_ENDIAN_BYTE_ORDER);
#endif
}


BinaryReader::BinaryReader(std::istream& istr, TextEncoding& encoding, StreamByteOrder byteOrder):
	_istr(istr),
	_pTextConverter(new TextConverter(encoding, Poco::TextEncoding::global()))
{
#if defined(POCO_ARCH_BIG_ENDIAN)
	_flipBytes = (byteOrder == LITTLE_ENDIAN_BYTE_ORDER);
#else
	_flipBytes = (byteOrder == BIG_ENDIAN_BYTE_ORDER);
#endif
}


BinaryReader::~BinaryReader()
{
	delete _pTextConverter;
}


BinaryReader& BinaryReader::operator >> (bool& value)
{
	_istr.read((char*) &value, sizeof(value));
	return *this;
}


BinaryReader& BinaryReader::operator >> (char& value)
{
	_istr.read((char*) &value, sizeof(value));
	return *this;
}


BinaryReader& BinaryReader::operator >> (unsigned char& value)
{
	_istr.read((char*) &value, sizeof(value));
	return *this;
}


BinaryReader& BinaryReader::operator >> (signed char& value)
{
	_istr.read((char*) &value, sizeof(value));
	return *this;
}


BinaryReader& BinaryReader::operator >> (short& value)
{
	_istr.read((char*) &value, sizeof(value));
	if (_flipBytes) value = ByteOrder::flipBytes(value);
	return *this;
}


BinaryReader& BinaryReader::operator >> (unsigned short& value)
{
	_istr.read((char*) &value, sizeof(value));
	if (_flipBytes) value = ByteOrder::flipBytes(value);
	return *this;
}


BinaryReader& BinaryReader::operator >> (int& value)
{
	_istr.read((char*) &value, sizeof(value));
	if (_flipBytes) value = ByteOrder::flipBytes(value);
	return *this;
}


BinaryReader& BinaryReader::operator >> (unsigned int& value)
{
	_istr.read((char*) &value, sizeof(value));
	if (_flipBytes) value = ByteOrder::flipBytes(value);
	return *this;
}


BinaryReader& BinaryReader::operator >> (long& value)
{
	_istr.read((char*) &value, sizeof(value));
#if defined(POCO_LONG_IS_64_BIT)
	if (_flipBytes) value = ByteOrder::flipBytes((Int64) value);
#else
	if (_flipBytes) value = ByteOrder::flipBytes((Int32) value);
#endif
	return *this;
}


BinaryReader& BinaryReader::operator >> (unsigned long& value)
{
	_istr.read((char*) &value, sizeof(value));
#if defined(POCO_LONG_IS_64_BIT)
	if (_flipBytes) value = ByteOrder::flipBytes((UInt64) value);
#else
	if (_flipBytes) value = ByteOrder::flipBytes((UInt32) value);
#endif
	return *this;
}


BinaryReader& BinaryReader::operator >> (float& value)
{
	if (_flipBytes)
	{
		char* ptr = (char*) &value;
		ptr += sizeof(value);
		for (unsigned i = 0; i < sizeof(value); ++i)
			_istr.read(--ptr, 1);
	}
	else
	{
		_istr.read((char*) &value, sizeof(value));
	}
	return *this;
}


BinaryReader& BinaryReader::operator >> (double& value)
{
	if (_flipBytes)
	{
		char* ptr = (char*) &value;
		ptr += sizeof(value);
		for (unsigned i = 0; i < sizeof(value); ++i)
			_istr.read(--ptr, 1);
	}
	else
	{
		_istr.read((char*) &value, sizeof(value));
	}
	return *this;
}


#if defined(POCO_HAVE_INT64) && !defined(POCO_LONG_IS_64_BIT)


BinaryReader& BinaryReader::operator >> (Int64& value)
{
	_istr.read((char*) &value, sizeof(value));
	if (_flipBytes) value = ByteOrder::flipBytes(value);
	return *this;
}


BinaryReader& BinaryReader::operator >> (UInt64& value)
{
	_istr.read((char*) &value, sizeof(value));
	if (_flipBytes) value = ByteOrder::flipBytes(value);
	return *this;
}


#endif


BinaryReader& BinaryReader::operator >> (std::string& value)
{
	UInt32 size = 0;
	read7BitEncoded(size);
	value.clear();
	if (!_istr.good()) return *this;
	value.reserve(size);
	while (size--)
	{
		char c;
		if (!_istr.read(&c, 1).good()) break;
		value += c;
	}
	if (_pTextConverter)
	{
		std::string converted;
		_pTextConverter->convert(value, converted);
		std::swap(value, converted);
	}
	return *this;
}


void BinaryReader::read7BitEncoded(UInt32& value)
{
	char c;
	value = 0;
	int s = 0;
	do
	{
		c = 0;
		_istr.read(&c, 1);
		UInt32 x = (c & 0x7F);
		x <<= s;
		value += x;
		s += 7;
	}
	while (c & 0x80);
}


#if defined(POCO_HAVE_INT64)


void BinaryReader::read7BitEncoded(UInt64& value)
{
	char c;
	value = 0;
	int s = 0;
	do
	{
		c = 0;
		_istr.read(&c, 1);
		UInt64 x = (c & 0x7F);
		x <<= s;
		value += x;
		s += 7;
	}
	while (c & 0x80);
}


#endif


void BinaryReader::readRaw(std::streamsize length, std::string& value)
{
	value.clear();
	value.reserve(static_cast<std::string::size_type>(length));
	while (length--)
	{
		char c;
		if (!_istr.read(&c, 1).good()) break;
		value += c;
	}
}


void BinaryReader::readRaw(char* buffer, std::streamsize length)
{
	_istr.read(buffer, length);
}


void BinaryReader::readBOM()
{
	UInt16 bom;
	_istr.read((char*) &bom, sizeof(bom));
	_flipBytes = bom != 0xFEFF;
}


} // namespace Poco
