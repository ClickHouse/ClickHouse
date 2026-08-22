//
// UTF32Encoding.cpp
//
// Library: Foundation
// Package: Text
// Module:  UTF32Encoding
//
// Copyright (c) 2004-2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/UTF32Encoding.h"
#include "Poco/ByteOrder.h"
#include "Poco/String.h"


namespace Poco {


const char* UTF32Encoding::_names[] =
{
	"UTF-32",
	"UTF32",
	NULL
};


const TextEncoding::CharacterMap UTF32Encoding::_charMap = 
{
	/* 00 */	-4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4,
	/* 10 */	-4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4,
	/* 20 */	-4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4,
	/* 30 */	-4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4,
	/* 40 */	-4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4,
	/* 50 */	-4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4,
	/* 60 */	-4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4,
	/* 70 */	-4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4,
	/* 80 */	-4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4,
	/* 90 */	-4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4,
	/* a0 */	-4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4,
	/* b0 */	-4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4,
	/* c0 */	-4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4,
	/* d0 */	-4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4,
	/* e0 */	-4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4,
	/* f0 */	-4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4,
};


UTF32Encoding::UTF32Encoding(ByteOrderType byteOrder)
{
	setByteOrder(byteOrder);
}

	
UTF32Encoding::UTF32Encoding(int byteOrderMark)
{
	setByteOrder(byteOrderMark);
}

	
UTF32Encoding::~UTF32Encoding()
{
}


UTF32Encoding::ByteOrderType UTF32Encoding::getByteOrder() const
{
#if defined(POCO_ARCH_BIG_ENDIAN)
	return _flipBytes ? LITTLE_ENDIAN_BYTE_ORDER : BIG_ENDIAN_BYTE_ORDER;
#else
	return _flipBytes ? BIG_ENDIAN_BYTE_ORDER : LITTLE_ENDIAN_BYTE_ORDER;
#endif
}

	
void UTF32Encoding::setByteOrder(ByteOrderType byteOrder)
{
#if defined(POCO_ARCH_BIG_ENDIAN)
	_flipBytes = byteOrder == LITTLE_ENDIAN_BYTE_ORDER;
#else
	_flipBytes = byteOrder == BIG_ENDIAN_BYTE_ORDER;;
#endif
}

	
void UTF32Encoding::setByteOrder(int byteOrderMark)
{
	_flipBytes = byteOrderMark != 0xFEFF;
}


const char* UTF32Encoding::canonicalName() const
{
	return _names[0];
}


bool UTF32Encoding::isA(const std::string& encodingName) const
{
	for (const char** name = _names; *name; ++name)
	{
		if (Poco::icompare(encodingName, *name) == 0)
			return true;
	}
	return false;
}


const TextEncoding::CharacterMap& UTF32Encoding::characterMap() const
{
	return _charMap;
}


int UTF32Encoding::convert(const unsigned char* bytes) const
{
	UInt32 uc;
	unsigned char* p = reinterpret_cast<unsigned char*>(&uc);
	*p++ = *bytes++;
	*p++ = *bytes++;
	*p++ = *bytes++;
	*p++ = *bytes++;

	if (_flipBytes)
	{
		ByteOrder::flipBytes(uc);
	}

	return safeToInt(uc);
}


int UTF32Encoding::convert(int ch, unsigned char* bytes, int length) const
{
	if (bytes && length >= 4)
	{
		UInt32 ch1 = _flipBytes ? ByteOrder::flipBytes((UInt32) ch) : (UInt32) ch;
		unsigned char* p = reinterpret_cast<unsigned char*>(&ch1);
		*bytes++ = *p++;
		*bytes++ = *p++;
		*bytes++ = *p++;
		*bytes++ = *p++;
	}
	return 4;
}


int UTF32Encoding::queryConvert(const unsigned char* bytes, int length) const
{
	int ret = -4;

	if (length >= 4)
	{
		UInt32 uc;
		unsigned char* p = reinterpret_cast<unsigned char*>(&uc);
		*p++ = *bytes++;
		*p++ = *bytes++;
		*p++ = *bytes++;
		*p++ = *bytes++;
		if (_flipBytes) 
			ByteOrder::flipBytes(uc);
		ret = safeToInt(uc);
	}

	return ret;
}


int UTF32Encoding::sequenceLength(const unsigned char* bytes, int length) const
{
	return 4;
}


} // namespace Poco
