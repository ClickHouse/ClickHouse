//
// UTF8Encoding.cpp
//
// $Id: //poco/1.4/Foundation/src/UTF8Encoding.cpp#1 $
//
// Library: Foundation
// Package: Text
// Module:  UTF8Encoding
//
// Copyright (c) 2004-2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/UTF8Encoding.h"
#include "Poco/String.h"


namespace Poco {


const char* UTF8Encoding::_names[] =
{
	"UTF-8",
	"UTF8",
	NULL
};


const TextEncoding::CharacterMap UTF8Encoding::_charMap = 
{
	/* 00 */	0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 
	/* 10 */	0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 
	/* 20 */	0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 
	/* 30 */	0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f, 
	/* 40 */	0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49, 0x4a, 0x4b, 0x4c, 0x4d, 0x4e, 0x4f, 
	/* 50 */	0x50, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 0x59, 0x5a, 0x5b, 0x5c, 0x5d, 0x5e, 0x5f, 
	/* 60 */	0x60, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a, 0x6b, 0x6c, 0x6d, 0x6e, 0x6f, 
	/* 70 */	0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78, 0x79, 0x7a, 0x7b, 0x7c, 0x7d, 0x7e, 0x7f, 
	/* 80 */	  -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1, 
	/* 90 */	  -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1, 
	/* a0 */	  -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1, 
	/* b0 */	  -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1, 
	/* c0 */	  -2,   -2,   -2,   -2,   -2,   -2,   -2,   -2,   -2,   -2,   -2,   -2,   -2,   -2,   -2,   -2, 
	/* d0 */	  -2,   -2,   -2,   -2,   -2,   -2,   -2,   -2,   -2,   -2,   -2,   -2,   -2,   -2,   -2,   -2, 
	/* e0 */	  -3,   -3,   -3,   -3,   -3,   -3,   -3,   -3,   -3,   -3,   -3,   -3,   -3,   -3,   -3,   -3, 
	/* f0 */	  -4,   -4,   -4,   -4,   -4,   -4,   -4,   -4,   -5,   -5,   -5,   -5,   -6,   -6,   -1,   -1, 
};


UTF8Encoding::UTF8Encoding()
{
}


UTF8Encoding::~UTF8Encoding()
{
}


const char* UTF8Encoding::canonicalName() const
{
	return _names[0];
}


bool UTF8Encoding::isA(const std::string& encodingName) const
{
	for (const char** name = _names; *name; ++name)
	{
		if (Poco::icompare(encodingName, *name) == 0)
			return true;
	}
	return false;
}


const TextEncoding::CharacterMap& UTF8Encoding::characterMap() const
{
	return _charMap;
}


int UTF8Encoding::convert(const unsigned char* bytes) const
{
	int n = _charMap[*bytes];
	int uc;
	
	switch (n)
	{
	case -6:
	case -5:
	case -1:
		return -1;
	case -4: 
	case -3: 
	case -2:
		if (!isLegal(bytes, -n)) return -1;
		uc = *bytes & ((0x07 << (n + 4)) | 0x03);
		break;
	default:
		return n;
	}

	while (n++ < -1) 
	{	
		uc <<= 6;
		uc |= (*++bytes & 0x3F);
	}
	return uc;
}


int UTF8Encoding::convert(int ch, unsigned char* bytes, int length) const
{
	if (ch <= 0x7F)
	{
		if (bytes && length >= 1)
			*bytes = (unsigned char) ch;
		return 1;
	}
	else if (ch <= 0x7FF)
	{
		if (bytes && length >= 2)
		{
			*bytes++ = (unsigned char) (((ch >> 6) & 0x1F) | 0xC0);
			*bytes   = (unsigned char) ((ch & 0x3F) | 0x80);
		}
		return 2;
	}
	else if (ch <= 0xFFFF)
	{
		if (bytes && length >= 3)
		{
			*bytes++ = (unsigned char) (((ch >> 12) & 0x0F) | 0xE0);
			*bytes++ = (unsigned char) (((ch >> 6) & 0x3F) | 0x80);
			*bytes   = (unsigned char) ((ch & 0x3F) | 0x80);
		}
		return 3;
	}
	else if (ch <= 0x10FFFF)
	{
		if (bytes && length >= 4)
		{
			*bytes++ = (unsigned char) (((ch >> 18) & 0x07) | 0xF0);
			*bytes++ = (unsigned char) (((ch >> 12) & 0x3F) | 0x80);
			*bytes++ = (unsigned char) (((ch >> 6) & 0x3F) | 0x80);
			*bytes   = (unsigned char) ((ch & 0x3F) | 0x80);
		}
		return 4;
	}
	else return 0;
}


int UTF8Encoding::queryConvert(const unsigned char* bytes, int length) const
{
	int n = _charMap[*bytes];
	int uc;
	if (-n > length)
	{
		return n;
	}
	else
	{
		switch (n)
		{
		case -6:
		case -5:
		case -1:
			return -1;
		case -4:
		case -3:
		case -2:
			if (!isLegal(bytes, -n)) return -1;
			uc = *bytes & ((0x07 << (n + 4)) | 0x03);
			break;
		default:
			return n;
		}
		while (n++ < -1) 
		{	
			uc <<= 6;
			uc |= (*++bytes & 0x3F);
		}
		return uc;
	}
}


int UTF8Encoding::sequenceLength(const unsigned char* bytes, int length) const
{
	if (1 <= length)
	{
		int cc = _charMap[*bytes];
		if (cc >= 0)
			return 1;
		else
			return -cc;
	}
	else return -1;
}


bool UTF8Encoding::isLegal(const unsigned char *bytes, int length)
{
	// Note: The following is loosely based on the isLegalUTF8 function
	// from ftp://ftp.unicode.org/Public/PROGRAMS/CVTUTF/ConvertUTF.c
	// Excuse the ugliness...
	
	if (0 == bytes || 0 == length) return false;

    unsigned char a;
    const unsigned char* srcptr = bytes + length;
    switch (length)
	{
	default:
		return false;
		// Everything else falls through when true.
	case 4:
		if ((a = (*--srcptr)) < 0x80 || a > 0xBF) return false;
	case 3: 
		if ((a = (*--srcptr)) < 0x80 || a > 0xBF) return false;
	case 2:
		if ((a = (*--srcptr)) > 0xBF) return false;
		switch (*bytes) 
		{
		case 0xE0:
			if (a < 0xA0) return false; 
			break;
		case 0xED:
			if (a > 0x9F) return false; 
			break;
		case 0xF0:
			if (a < 0x90) return false; 
			break;
		case 0xF4:
			if (a > 0x8F) return false; 
			break;
		default:
			if (a < 0x80) return false;
		}
	case 1:
		if (*bytes >= 0x80 && *bytes < 0xC2) return false;
    }
	return *bytes <= 0xF4;
}


} // namespace Poco
