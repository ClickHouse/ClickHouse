//
// Latin2Encoding.cpp
//
// $Id: //poco/1.4/Foundation/src/Latin2Encoding.cpp#1 $
//
// Library: Foundation
// Package: Text
// Module:  Latin2Encoding
//
// Copyright (c) 2004-2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Latin2Encoding.h"
#include "Poco/String.h"


namespace Poco {


const char* Latin2Encoding::_names[] =
{
	"ISO-8859-2",
	"Latin2",
	"Latin-2",
	NULL
};


const TextEncoding::CharacterMap Latin2Encoding::_charMap = 
{
	/*          00      01      02      03      04      05      06      07      08      09      0a      0b      0c      0d      0e      0f   */
	/* 00 */	0x0000, 0x0001, 0x0002, 0x0003, 0x0004, 0x0005, 0x0006, 0x0007, 0x0008, 0x0009, 0x000a, 0x000b, 0x000c, 0x000d, 0x000e, 0x000f, 
	/* 10 */	0x0010, 0x0011, 0x0012, 0x0013, 0x0014, 0x0015, 0x0016, 0x0017, 0x0018, 0x0019, 0x001a, 0x001b, 0x001c, 0x001d, 0x001e, 0x001f, 
	/* 20 */	0x0020, 0x0021, 0x0022, 0x0023, 0x0024, 0x0025, 0x0026, 0x0027, 0x0028, 0x0029, 0x002a, 0x002b, 0x002c, 0x002d, 0x002e, 0x002f, 
	/* 30 */	0x0030, 0x0031, 0x0032, 0x0033, 0x0034, 0x0035, 0x0036, 0x0037, 0x0038, 0x0039, 0x003a, 0x003b, 0x003c, 0x003d, 0x003e, 0x003f, 
	/* 40 */	0x0040, 0x0041, 0x0042, 0x0043, 0x0044, 0x0045, 0x0046, 0x0047, 0x0048, 0x0049, 0x004a, 0x004b, 0x004c, 0x004d, 0x004e, 0x004f, 
	/* 50 */	0x0050, 0x0051, 0x0052, 0x0053, 0x0054, 0x0055, 0x0056, 0x0057, 0x0058, 0x0059, 0x005a, 0x005b, 0x005c, 0x005d, 0x005e, 0x005f, 
	/* 60 */	0x0060, 0x0061, 0x0062, 0x0063, 0x0064, 0x0065, 0x0066, 0x0067, 0x0068, 0x0069, 0x006a, 0x006b, 0x006c, 0x006d, 0x006e, 0x006f, 
	/* 70 */	0x0070, 0x0071, 0x0072, 0x0073, 0x0074, 0x0075, 0x0076, 0x0077, 0x0078, 0x0079, 0x007a, 0x007b, 0x007c, 0x007d, 0x007e, 0x007f, 
	/* 80 */	0x0080, 0x0081, 0x0082, 0x0083, 0x0084, 0x0085, 0x0086, 0x0087, 0x0088, 0x0089, 0x008a, 0x008b, 0x008c, 0x008d, 0x008e, 0x008f, 
	/* 90 */	0x0090, 0x0091, 0x0092, 0x0093, 0x0094, 0x0095, 0x0096, 0x0097, 0x0098, 0x0099, 0x009a, 0x009b, 0x009c, 0x009d, 0x009e, 0x009f, 
	/* a0 */	0x00a0, 0x0104, 0x02d8, 0x0141, 0x00a4, 0x013d, 0x015a, 0x00a7, 0x00a8, 0x0160, 0x015e, 0x0164, 0x0179, 0x00ad, 0x017d, 0x017b,
	/* b0 */	0x00b0, 0x0105, 0x02db, 0x0142, 0x00b4, 0x013e, 0x015b, 0x02c7, 0x00b8, 0x0161, 0x015f, 0x0165, 0x017a, 0x02dd, 0x017e, 0x017c,
	/* c0 */	0x0154, 0x00c1, 0x00c2, 0x0102, 0x00c4, 0x0139, 0x0106, 0x00c7, 0x010c, 0x00c9, 0x0118, 0x00cb, 0x011a, 0x00cd, 0x00ce, 0x010e,
	/* d0 */	0x0110, 0x0143, 0x0147, 0x00d3, 0x00d4, 0x0150, 0x00d6, 0x00d7, 0x0158, 0x016e, 0x00da, 0x0170, 0x00dc, 0x00dd, 0x0162, 0x00df,
	/* e0 */	0x0155, 0x00e1, 0x00e2, 0x0103, 0x00e4, 0x013a, 0x0107, 0x00e7, 0x010d, 0x00e9, 0x0119, 0x00eb, 0x011b, 0x00ed, 0x00ee, 0x010f,
	/* f0 */	0x0111, 0x0144, 0x0148, 0x00f3, 0x00f4, 0x0151, 0x00f6, 0x00f7, 0x0159, 0x016f, 0x00fa, 0x0171, 0x00fc, 0x00fd, 0x0163, 0x02d9,
};


Latin2Encoding::Latin2Encoding()
{
}


Latin2Encoding::~Latin2Encoding()
{
}


const char* Latin2Encoding::canonicalName() const
{
	return _names[0];
}


bool Latin2Encoding::isA(const std::string& encodingName) const
{
	for (const char** name = _names; *name; ++name)
	{
		if (Poco::icompare(encodingName, *name) == 0)
			return true;
	}
	return false;
}


const TextEncoding::CharacterMap& Latin2Encoding::characterMap() const
{
	return _charMap;
}


int Latin2Encoding::convert(const unsigned char* bytes) const
{
	return _charMap[*bytes];
}


int Latin2Encoding::convert(int ch, unsigned char* bytes, int length) const
{
	if (ch >= 0 && ch <= 255 && _charMap[ch] == ch)
	{
		if (bytes && length >= 1)
			*bytes = (unsigned char) ch;
		return 1;
	}
	switch(ch)
	{
	case 0x0104: if (bytes && length >= 1) *bytes = 0xa1; return 1;
	case 0x02d8: if (bytes && length >= 1) *bytes = 0xa2; return 1;
	case 0x0141: if (bytes && length >= 1) *bytes = 0xa3; return 1;
	case 0x013d: if (bytes && length >= 1) *bytes = 0xa5; return 1;
	case 0x015a: if (bytes && length >= 1) *bytes = 0xa6; return 1;
	case 0x0160: if (bytes && length >= 1) *bytes = 0xa9; return 1;
	case 0x015e: if (bytes && length >= 1) *bytes = 0xaa; return 1;
	case 0x0164: if (bytes && length >= 1) *bytes = 0xab; return 1;
	case 0x0179: if (bytes && length >= 1) *bytes = 0xac; return 1;
	case 0x017d: if (bytes && length >= 1) *bytes = 0xae; return 1;
	case 0x017b: if (bytes && length >= 1) *bytes = 0xaf; return 1;
	case 0x0105: if (bytes && length >= 1) *bytes = 0xb1; return 1;
	case 0x02db: if (bytes && length >= 1) *bytes = 0xb2; return 1;
	case 0x0142: if (bytes && length >= 1) *bytes = 0xb3; return 1;
	case 0x013e: if (bytes && length >= 1) *bytes = 0xb5; return 1;
	case 0x015b: if (bytes && length >= 1) *bytes = 0xb6; return 1;
	case 0x02c7: if (bytes && length >= 1) *bytes = 0xb7; return 1;
	case 0x0161: if (bytes && length >= 1) *bytes = 0xb9; return 1;
	case 0x015f: if (bytes && length >= 1) *bytes = 0xba; return 1;
	case 0x0165: if (bytes && length >= 1) *bytes = 0xbb; return 1;
	case 0x017a: if (bytes && length >= 1) *bytes = 0xbc; return 1;
	case 0x02dd: if (bytes && length >= 1) *bytes = 0xbd; return 1;
	case 0x017e: if (bytes && length >= 1) *bytes = 0xbe; return 1;
	case 0x017c: if (bytes && length >= 1) *bytes = 0xbf; return 1;
	case 0x0154: if (bytes && length >= 1) *bytes = 0xc0; return 1;
	case 0x0102: if (bytes && length >= 1) *bytes = 0xc3; return 1;
	case 0x0139: if (bytes && length >= 1) *bytes = 0xc5; return 1;
	case 0x0106: if (bytes && length >= 1) *bytes = 0xc6; return 1;
	case 0x010c: if (bytes && length >= 1) *bytes = 0xc8; return 1;
	case 0x0118: if (bytes && length >= 1) *bytes = 0xca; return 1;
	case 0x011a: if (bytes && length >= 1) *bytes = 0xcc; return 1;
	case 0x010e: if (bytes && length >= 1) *bytes = 0xcf; return 1;
	case 0x0110: if (bytes && length >= 1) *bytes = 0xd0; return 1;
	case 0x0143: if (bytes && length >= 1) *bytes = 0xd1; return 1;
	case 0x0147: if (bytes && length >= 1) *bytes = 0xd2; return 1;
	case 0x0150: if (bytes && length >= 1) *bytes = 0xd5; return 1;
	case 0x0158: if (bytes && length >= 1) *bytes = 0xd8; return 1;
	case 0x016e: if (bytes && length >= 1) *bytes = 0xd9; return 1;
	case 0x0170: if (bytes && length >= 1) *bytes = 0xdb; return 1;
	case 0x0162: if (bytes && length >= 1) *bytes = 0xde; return 1;
	case 0x0155: if (bytes && length >= 1) *bytes = 0xe0; return 1;
	case 0x0103: if (bytes && length >= 1) *bytes = 0xe3; return 1;
	case 0x013a: if (bytes && length >= 1) *bytes = 0xe5; return 1;
	case 0x0107: if (bytes && length >= 1) *bytes = 0xe6; return 1;
	case 0x010d: if (bytes && length >= 1) *bytes = 0xe8; return 1;
	case 0x0119: if (bytes && length >= 1) *bytes = 0xea; return 1;
	case 0x011b: if (bytes && length >= 1) *bytes = 0xec; return 1;
	case 0x010f: if (bytes && length >= 1) *bytes = 0xef; return 1;
	case 0x0111: if (bytes && length >= 1) *bytes = 0xf0; return 1;
	case 0x0144: if (bytes && length >= 1) *bytes = 0xf1; return 1;
	case 0x0148: if (bytes && length >= 1) *bytes = 0xf2; return 1;
	case 0x0151: if (bytes && length >= 1) *bytes = 0xf5; return 1;
	case 0x0159: if (bytes && length >= 1) *bytes = 0xf8; return 1;
	case 0x016f: if (bytes && length >= 1) *bytes = 0xf9; return 1;
	case 0x0171: if (bytes && length >= 1) *bytes = 0xfb; return 1;
	case 0x0163: if (bytes && length >= 1) *bytes = 0xfe; return 1;
	default: return 0;
	}
}


int Latin2Encoding::queryConvert(const unsigned char* bytes, int length) const
{
	if (1 <= length)
		return _charMap[*bytes];
	else
		return -1;
}


int Latin2Encoding::sequenceLength(const unsigned char* bytes, int length) const
{
	return 1;
}


} // namespace Poco
