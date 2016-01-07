//
// BinaryWriter.cpp
//
// $Id: //poco/1.4/Foundation/src/BinaryWriter.cpp#1 $
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


#include "Poco/BinaryWriter.h"
#include "Poco/ByteOrder.h"
#include "Poco/TextEncoding.h"
#include "Poco/TextConverter.h"
#include <cstring>


namespace Poco {


BinaryWriter::BinaryWriter(std::ostream& ostr, StreamByteOrder byteOrder):
	_ostr(ostr),
	_pTextConverter(0)
{
#if defined(POCO_ARCH_BIG_ENDIAN)
	_flipBytes = (byteOrder == LITTLE_ENDIAN_BYTE_ORDER);
#else
	_flipBytes = (byteOrder == BIG_ENDIAN_BYTE_ORDER);
#endif
}


BinaryWriter::BinaryWriter(std::ostream& ostr, TextEncoding& encoding, StreamByteOrder byteOrder):
	_ostr(ostr),
	_pTextConverter(new TextConverter(Poco::TextEncoding::global(), encoding))
{
#if defined(POCO_ARCH_BIG_ENDIAN)
	_flipBytes = (byteOrder == LITTLE_ENDIAN_BYTE_ORDER);
#else
	_flipBytes = (byteOrder == BIG_ENDIAN_BYTE_ORDER);
#endif
}


BinaryWriter::~BinaryWriter()
{
	delete _pTextConverter;
}


BinaryWriter& BinaryWriter::operator << (bool value)
{
	_ostr.write((const char*) &value, sizeof(value));
	return *this;
}


BinaryWriter& BinaryWriter::operator << (char value)
{
	_ostr.write((const char*) &value, sizeof(value));
	return *this;
}


BinaryWriter& BinaryWriter::operator << (unsigned char value)
{
	_ostr.write((const char*) &value, sizeof(value));
	return *this;
}


BinaryWriter& BinaryWriter::operator << (signed char value)
{
	_ostr.write((const char*) &value, sizeof(value));
	return *this;
}


BinaryWriter& BinaryWriter::operator << (short value)
{
	if (_flipBytes)
	{
		short fValue = ByteOrder::flipBytes(value);
		_ostr.write((const char*) &fValue, sizeof(fValue));
	}
	else
	{
		_ostr.write((const char*) &value, sizeof(value));
	}
	return *this;
}


BinaryWriter& BinaryWriter::operator << (unsigned short value)
{
	if (_flipBytes)
	{
		unsigned short fValue = ByteOrder::flipBytes(value);
		_ostr.write((const char*) &fValue, sizeof(fValue));
	}
	else
	{
		_ostr.write((const char*) &value, sizeof(value));
	}
	return *this;
}


BinaryWriter& BinaryWriter::operator << (int value)
{
	if (_flipBytes)
	{
		int fValue = ByteOrder::flipBytes(value);
		_ostr.write((const char*) &fValue, sizeof(fValue));
	}
	else
	{
		_ostr.write((const char*) &value, sizeof(value));
	}
	return *this;
}


BinaryWriter& BinaryWriter::operator << (unsigned int value)
{
	if (_flipBytes)
	{
		unsigned int fValue = ByteOrder::flipBytes(value);
		_ostr.write((const char*) &fValue, sizeof(fValue));
	}
	else
	{
		_ostr.write((const char*) &value, sizeof(value));
	}
	return *this;
}


BinaryWriter& BinaryWriter::operator << (long value)
{
	if (_flipBytes)
	{
#if defined(POCO_LONG_IS_64_BIT)
		long fValue = ByteOrder::flipBytes((Int64) value);
#else
		long fValue = ByteOrder::flipBytes((Int32) value);
#endif
		_ostr.write((const char*) &fValue, sizeof(fValue));
	}
	else
	{
		_ostr.write((const char*) &value, sizeof(value));
	}
	return *this;
}


BinaryWriter& BinaryWriter::operator << (unsigned long value)
{
	if (_flipBytes)
	{
#if defined(POCO_LONG_IS_64_BIT)
		long fValue = ByteOrder::flipBytes((UInt64) value);
#else
		long fValue = ByteOrder::flipBytes((UInt32) value);
#endif
		_ostr.write((const char*) &fValue, sizeof(fValue));
	}
	else
	{
		_ostr.write((const char*) &value, sizeof(value));
	}
	return *this;
}


BinaryWriter& BinaryWriter::operator << (float value)
{
	if (_flipBytes)
	{
		const char* ptr = (const char*) &value;
		ptr += sizeof(value);
		for (unsigned i = 0; i < sizeof(value); ++i)
			_ostr.write(--ptr, 1);
	}
	else
	{
		_ostr.write((const char*) &value, sizeof(value));
	}
	return *this;
}


BinaryWriter& BinaryWriter::operator << (double value)
{
	if (_flipBytes)
	{
		const char* ptr = (const char*) &value;
		ptr += sizeof(value);
		for (unsigned i = 0; i < sizeof(value); ++i)
			_ostr.write(--ptr, 1);
	}
	else
	{
		_ostr.write((const char*) &value, sizeof(value));
	}
	return *this;
}


#if defined(POCO_HAVE_INT64) && !defined(POCO_LONG_IS_64_BIT)


BinaryWriter& BinaryWriter::operator << (Int64 value)
{
	if (_flipBytes)
	{
		Int64 fValue = ByteOrder::flipBytes(value);
		_ostr.write((const char*) &fValue, sizeof(fValue));
	}
	else
	{
		_ostr.write((const char*) &value, sizeof(value));
	}
	return *this;
}


BinaryWriter& BinaryWriter::operator << (UInt64 value)
{
	if (_flipBytes)
	{
		UInt64 fValue = ByteOrder::flipBytes(value);
		_ostr.write((const char*) &fValue, sizeof(fValue));
	}
	else
	{
		_ostr.write((const char*) &value, sizeof(value));
	}
	return *this;
}


#endif


BinaryWriter& BinaryWriter::operator << (const std::string& value)
{
	if (_pTextConverter)
	{
		std::string converted;
		_pTextConverter->convert(value, converted);
		UInt32 length = (UInt32) converted.size();
		write7BitEncoded(length);
		_ostr.write(converted.data(), length);
	}
	else
	{
		UInt32 length = (UInt32) value.size();
		write7BitEncoded(length);
		_ostr.write(value.data(), length);
	}
	return *this;
}


BinaryWriter& BinaryWriter::operator << (const char* value)
{
	poco_check_ptr (value);
	
	if (_pTextConverter)
	{
		std::string converted;
		_pTextConverter->convert(value, static_cast<int>(std::strlen(value)), converted);
		UInt32 length = (UInt32) converted.size();
		write7BitEncoded(length);
		_ostr.write(converted.data(), length);
	}
	else
	{
		UInt32 length = static_cast<UInt32>(std::strlen(value));
		write7BitEncoded(length);
		_ostr.write(value, length);
	}
	return *this;
}


void BinaryWriter::write7BitEncoded(UInt32 value)
{
	do
	{
		unsigned char c = (unsigned char) (value & 0x7F);
		value >>= 7;
		if (value) c |= 0x80;
		_ostr.write((const char*) &c, 1);
	}
	while (value);
}


#if defined(POCO_HAVE_INT64)


void BinaryWriter::write7BitEncoded(UInt64 value)
{
	do
	{
		unsigned char c = (unsigned char) (value & 0x7F);
		value >>= 7;
		if (value) c |= 0x80;
		_ostr.write((const char*) &c, 1);
	}
	while (value);
}


#endif


void BinaryWriter::writeRaw(const std::string& rawData)
{
	_ostr.write(rawData.data(), (std::streamsize) rawData.length());
}


void BinaryWriter::writeRaw(const char* buffer, std::streamsize length)
{
	_ostr.write(buffer, length);
}


void BinaryWriter::writeBOM()
{
	UInt16 value = 0xFEFF;
	if (_flipBytes) value = ByteOrder::flipBytes(value);
	_ostr.write((const char*) &value, sizeof(value));
}


void BinaryWriter::flush()
{
	_ostr.flush();
}


} // namespace Poco
