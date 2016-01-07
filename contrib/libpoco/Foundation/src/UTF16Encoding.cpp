//
// UTF16Encoding.cpp
//
// $Id: //poco/1.4/Foundation/src/UTF16Encoding.cpp#1 $
//
// Library: Foundation
// Package: Text
// Module:  UTF16Encoding
//
// Copyright (c) 2004-2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/UTF16Encoding.h"
#include "Poco/ByteOrder.h"
#include "Poco/String.h"


namespace Poco {


const char* UTF16Encoding::_names[] =
{
	"UTF-16",
	"UTF16",
	NULL
};


const TextEncoding::CharacterMap UTF16Encoding::_charMap = 
{
	/* 00 */	-2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, 
	/* 10 */	-2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, 
	/* 20 */	-2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, 
	/* 30 */	-2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, 
	/* 40 */	-2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, 
	/* 50 */	-2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, 
	/* 60 */	-2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, 
	/* 70 */	-2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, 
	/* 80 */	-2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, 
	/* 90 */	-2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, 
	/* a0 */	-2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, 
	/* b0 */	-2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, 
	/* c0 */	-2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, 
	/* d0 */	-2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, 
	/* e0 */	-2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, 
	/* f0 */	-2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, 
};


UTF16Encoding::UTF16Encoding(ByteOrderType byteOrder)
{
	setByteOrder(byteOrder);
}

	
UTF16Encoding::UTF16Encoding(int byteOrderMark)
{
	setByteOrder(byteOrderMark);
}

	
UTF16Encoding::~UTF16Encoding()
{
}


UTF16Encoding::ByteOrderType UTF16Encoding::getByteOrder() const
{
#if defined(POCO_ARCH_BIG_ENDIAN)
	return _flipBytes ? LITTLE_ENDIAN_BYTE_ORDER : BIG_ENDIAN_BYTE_ORDER;
#else
	return _flipBytes ? BIG_ENDIAN_BYTE_ORDER : LITTLE_ENDIAN_BYTE_ORDER;
#endif
}

	
void UTF16Encoding::setByteOrder(ByteOrderType byteOrder)
{
#if defined(POCO_ARCH_BIG_ENDIAN)
	_flipBytes = byteOrder == LITTLE_ENDIAN_BYTE_ORDER;
#else
	_flipBytes = byteOrder == BIG_ENDIAN_BYTE_ORDER;;
#endif
}

	
void UTF16Encoding::setByteOrder(int byteOrderMark)
{
	_flipBytes = byteOrderMark != 0xFEFF;
}


const char* UTF16Encoding::canonicalName() const
{
	return _names[0];
}


bool UTF16Encoding::isA(const std::string& encodingName) const
{
	for (const char** name = _names; *name; ++name)
	{
		if (Poco::icompare(encodingName, *name) == 0)
			return true;
	}
	return false;
}


const TextEncoding::CharacterMap& UTF16Encoding::characterMap() const
{
	return _charMap;
}


int UTF16Encoding::convert(const unsigned char* bytes) const
{
	UInt16 uc;
	unsigned char* p = (unsigned char*) &uc;
	*p++ = *bytes++;
	*p++ = *bytes++;

	if (_flipBytes)
	{
		ByteOrder::flipBytes(uc);
	}

	if (uc >= 0xd800 && uc < 0xdc00)
	{
		UInt16 uc2;
		p = (unsigned char*) &uc2;
		*p++ = *bytes++;
		*p++ = *bytes++;

		if (_flipBytes)
		{
			ByteOrder::flipBytes(uc2);
		}
		if (uc2 >= 0xdc00 && uc2 < 0xe000)
		{
			return ((uc & 0x3ff) << 10) + (uc2 & 0x3ff) + 0x10000;
		}
		else
		{
			return -1;
		}
	}
	else
	{
		return uc;
	}
}


int UTF16Encoding::convert(int ch, unsigned char* bytes, int length) const
{
	if (ch <= 0xFFFF)
	{
		if (bytes && length >= 2)
		{
			UInt16 ch1 = _flipBytes ? ByteOrder::flipBytes((UInt16) ch) : (UInt16) ch;
			unsigned char* p = (unsigned char*) &ch1;
			*bytes++ = *p++;
			*bytes++ = *p++;
		}
		return 2;
	}
	else
	{
		if (bytes && length >= 4)
		{
			int ch1 = ch - 0x10000;
			UInt16 w1 = 0xD800 + ((ch1 >> 10) & 0x3FF);
			UInt16 w2 = 0xDC00 + (ch1 & 0x3FF);
			if (_flipBytes)
			{
				w1 = ByteOrder::flipBytes(w1);
				w2 = ByteOrder::flipBytes(w2);
			}
			unsigned char* p = (unsigned char*) &w1;
			*bytes++ = *p++;
			*bytes++ = *p++;
			p = (unsigned char*) &w2;
			*bytes++ = *p++;
			*bytes++ = *p++;
		}
		return 4;
	}
}


int UTF16Encoding::queryConvert(const unsigned char* bytes, int length) const
{
	int ret = -2;

	if (length >= 2)
	{
		UInt16 uc;
		unsigned char* p = (unsigned char*) &uc;
		*p++ = *bytes++;
		*p++ = *bytes++;
		if (_flipBytes) 
			ByteOrder::flipBytes(uc);
		if (uc >= 0xd800 && uc < 0xdc00)
		{
			if (length >= 4)
			{
				UInt16 uc2;
				p = (unsigned char*) &uc2;
				*p++ = *bytes++;
				*p++ = *bytes++;
				if (_flipBytes) 
					ByteOrder::flipBytes(uc2);
				if (uc2 >= 0xdc00 && uc < 0xe000)
				{
					ret = ((uc & 0x3ff) << 10) + (uc2 & 0x3ff) + 0x10000;
				}
				else
				{
					ret = -1;	// Malformed sequence
				}
			}
			else
			{
				ret = -4;	// surrogate pair, four bytes needed
			}
		}
		else
		{
			ret = uc;
		}
	}

	return ret;
}


int UTF16Encoding::sequenceLength(const unsigned char* bytes, int length) const
{
	int ret = -2;

	if (_flipBytes)
	{
		if (length >= 1)
		{
			unsigned char c = *bytes;
			if (c >= 0xd8 && c < 0xdc)
				ret = 4;
			else
				ret = 2;
		}
	}
	else
	{
		if (length >= 2)
		{
			UInt16 uc;
			unsigned char* p = (unsigned char*) &uc;
			*p++ = *bytes++;
			*p++ = *bytes++;
			if (uc >= 0xd800 && uc < 0xdc00)
				ret = 4;
			else
				ret = 2;
		}
	}
	return ret;
}


} // namespace Poco
