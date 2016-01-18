//
// Windows1251Encoding.cpp
//
// $Id: //poco/1.4/Foundation/src/Windows1251Encoding.cpp#1 $
//
// Library: Foundation
// Package: Text
// Module:  Windows1251Encoding
//
// Copyright (c) 2005-2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Windows1251Encoding.h"
#include "Poco/String.h"


namespace Poco {


const char* Windows1251Encoding::_names[] =
{
	"windows-1251",
	"Windows-1251",
	"cp1251",
	"CP1251",
	NULL
};


const TextEncoding::CharacterMap Windows1251Encoding::_charMap =
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
	/* 80 */	0x0402, 0x0403, 0x201a, 0x0453, 0x201e, 0x2026, 0x2020, 0x2021, 0x20ac, 0x2030, 0x0409, 0x2039, 0x040a, 0x040c, 0x040b, 0x040f,
	/* 90 */	0x0452, 0x2018, 0x2019, 0x201c, 0x201d, 0x2022, 0x2013, 0x2014, 0xfffe, 0x2122, 0x0459, 0x203a, 0x045a, 0x045c, 0x045b, 0x045f,
	/* a0 */	0x00a0, 0x040e, 0x045e, 0x0408, 0x00a4, 0x0490, 0x00a6, 0x00a7, 0x0401, 0x00a9, 0x0404, 0x00ab, 0x00ac, 0x00ad, 0x00ae, 0x0407,
	/* b0 */	0x00b0, 0x00b1, 0x0406, 0x0456, 0x0491, 0x00b5, 0x00b6, 0x00b7, 0x0451, 0x2116, 0x0454, 0x00bb, 0x0458, 0x0405, 0x0455, 0x0457,
	/* c0 */	0x0410, 0x0411, 0x0412, 0x0413, 0x0414, 0x0415, 0x0416, 0x0417, 0x0418, 0x0419, 0x041a, 0x041b, 0x041c, 0x041d, 0x041e, 0x041f,
	/* d0 */	0x0420, 0x0421, 0x0422, 0x0423, 0x0424, 0x0425, 0x0426, 0x0427, 0x0428, 0x0429, 0x042a, 0x042b, 0x042c, 0x042d, 0x042e, 0x042f,
	/* e0 */	0x0430, 0x0431, 0x0432, 0x0433, 0x0434, 0x0435, 0x0436, 0x0437, 0x0438, 0x0439, 0x043a, 0x043b, 0x043c, 0x043d, 0x043e, 0x043f,
	/* f0 */	0x0440, 0x0441, 0x0442, 0x0443, 0x0444, 0x0445, 0x0446, 0x0447, 0x0448, 0x0449, 0x044a, 0x044b, 0x044c, 0x044d, 0x044e, 0x044f,
};


Windows1251Encoding::Windows1251Encoding()
{
}


Windows1251Encoding::~Windows1251Encoding()
{
}


const char* Windows1251Encoding::canonicalName() const
{
	return _names[0];
}


bool Windows1251Encoding::isA(const std::string& encodingName) const
{
	for (const char** name = _names; *name; ++name)
	{
		if (Poco::icompare(encodingName, *name) == 0)
			return true;
	}
	return false;
}


const TextEncoding::CharacterMap& Windows1251Encoding::characterMap() const
{
	return _charMap;
}


int Windows1251Encoding::convert(const unsigned char* bytes) const
{
	return _charMap[*bytes];
}


int Windows1251Encoding::convert(int ch, unsigned char* bytes, int length) const
{
	if (ch >= 0 && ch <= 255 && _charMap[ch] == ch)
	{
		if (bytes && length >= 1)
			*bytes = (unsigned char) ch;
		return 1;
	}
	else switch(ch)
	{
	case 0x0402: if (bytes && length >= 1) *bytes = 0x80; return 1;
	case 0x0403: if (bytes && length >= 1) *bytes = 0x81; return 1;
	case 0x201a: if (bytes && length >= 1) *bytes = 0x82; return 1;
	case 0x0453: if (bytes && length >= 1) *bytes = 0x83; return 1;
	case 0x201e: if (bytes && length >= 1) *bytes = 0x84; return 1;
	case 0x2026: if (bytes && length >= 1) *bytes = 0x85; return 1;
	case 0x2020: if (bytes && length >= 1) *bytes = 0x86; return 1;
	case 0x2021: if (bytes && length >= 1) *bytes = 0x87; return 1;
	case 0x20ac: if (bytes && length >= 1) *bytes = 0x88; return 1;
	case 0x2030: if (bytes && length >= 1) *bytes = 0x89; return 1;
	case 0x0409: if (bytes && length >= 1) *bytes = 0x8a; return 1;
	case 0x2039: if (bytes && length >= 1) *bytes = 0x8b; return 1;
	case 0x040a: if (bytes && length >= 1) *bytes = 0x8c; return 1;
	case 0x040c: if (bytes && length >= 1) *bytes = 0x8d; return 1;
	case 0x040b: if (bytes && length >= 1) *bytes = 0x8e; return 1;
	case 0x040f: if (bytes && length >= 1) *bytes = 0x8f; return 1;
	case 0x0452: if (bytes && length >= 1) *bytes = 0x90; return 1;
	case 0x2018: if (bytes && length >= 1) *bytes = 0x91; return 1;
	case 0x2019: if (bytes && length >= 1) *bytes = 0x92; return 1;
	case 0x201c: if (bytes && length >= 1) *bytes = 0x93; return 1;
	case 0x201d: if (bytes && length >= 1) *bytes = 0x94; return 1;
	case 0x2022: if (bytes && length >= 1) *bytes = 0x95; return 1;
	case 0x2013: if (bytes && length >= 1) *bytes = 0x96; return 1;
	case 0x2014: if (bytes && length >= 1) *bytes = 0x97; return 1;
	case 0xfffe: if (bytes && length >= 1) *bytes = 0x98; return 1;
	case 0x2122: if (bytes && length >= 1) *bytes = 0x99; return 1;
	case 0x0459: if (bytes && length >= 1) *bytes = 0x9a; return 1;
	case 0x203a: if (bytes && length >= 1) *bytes = 0x9b; return 1;
	case 0x045a: if (bytes && length >= 1) *bytes = 0x9c; return 1;
	case 0x045c: if (bytes && length >= 1) *bytes = 0x9d; return 1;
	case 0x045b: if (bytes && length >= 1) *bytes = 0x9e; return 1;
	case 0x045f: if (bytes && length >= 1) *bytes = 0x9f; return 1;
	case 0x040e: if (bytes && length >= 1) *bytes = 0xa1; return 1;
	case 0x045e: if (bytes && length >= 1) *bytes = 0xa2; return 1;
	case 0x0408: if (bytes && length >= 1) *bytes = 0xa3; return 1;
	case 0x0490: if (bytes && length >= 1) *bytes = 0xa5; return 1;
	case 0x0401: if (bytes && length >= 1) *bytes = 0xa8; return 1;
	case 0x0404: if (bytes && length >= 1) *bytes = 0xaa; return 1;
	case 0x0407: if (bytes && length >= 1) *bytes = 0xaf; return 1;
	case 0x0406: if (bytes && length >= 1) *bytes = 0xb2; return 1;
	case 0x0456: if (bytes && length >= 1) *bytes = 0xb3; return 1;
	case 0x0491: if (bytes && length >= 1) *bytes = 0xb4; return 1;
	case 0x0451: if (bytes && length >= 1) *bytes = 0xb8; return 1;
	case 0x2116: if (bytes && length >= 1) *bytes = 0xb9; return 1;
	case 0x0454: if (bytes && length >= 1) *bytes = 0xba; return 1;
	case 0x0458: if (bytes && length >= 1) *bytes = 0xbc; return 1;
	case 0x0405: if (bytes && length >= 1) *bytes = 0xbd; return 1;
	case 0x0455: if (bytes && length >= 1) *bytes = 0xbe; return 1;
	case 0x0457: if (bytes && length >= 1) *bytes = 0xbf; return 1;
	case 0x0410: if (bytes && length >= 1) *bytes = 0xc0; return 1;
	case 0x0411: if (bytes && length >= 1) *bytes = 0xc1; return 1;
	case 0x0412: if (bytes && length >= 1) *bytes = 0xc2; return 1;
	case 0x0413: if (bytes && length >= 1) *bytes = 0xc3; return 1;
	case 0x0414: if (bytes && length >= 1) *bytes = 0xc4; return 1;
	case 0x0415: if (bytes && length >= 1) *bytes = 0xc5; return 1;
	case 0x0416: if (bytes && length >= 1) *bytes = 0xc6; return 1;
	case 0x0417: if (bytes && length >= 1) *bytes = 0xc7; return 1;
	case 0x0418: if (bytes && length >= 1) *bytes = 0xc8; return 1;
	case 0x0419: if (bytes && length >= 1) *bytes = 0xc9; return 1;
	case 0x041a: if (bytes && length >= 1) *bytes = 0xca; return 1;
	case 0x041b: if (bytes && length >= 1) *bytes = 0xcb; return 1;
	case 0x041c: if (bytes && length >= 1) *bytes = 0xcc; return 1;
	case 0x041d: if (bytes && length >= 1) *bytes = 0xcd; return 1;
	case 0x041e: if (bytes && length >= 1) *bytes = 0xce; return 1;
	case 0x041f: if (bytes && length >= 1) *bytes = 0xcf; return 1;
	case 0x0420: if (bytes && length >= 1) *bytes = 0xd0; return 1;
	case 0x0421: if (bytes && length >= 1) *bytes = 0xd1; return 1;
	case 0x0422: if (bytes && length >= 1) *bytes = 0xd2; return 1;
	case 0x0423: if (bytes && length >= 1) *bytes = 0xd3; return 1;
	case 0x0424: if (bytes && length >= 1) *bytes = 0xd4; return 1;
	case 0x0425: if (bytes && length >= 1) *bytes = 0xd5; return 1;
	case 0x0426: if (bytes && length >= 1) *bytes = 0xd6; return 1;
	case 0x0427: if (bytes && length >= 1) *bytes = 0xd7; return 1;
	case 0x0428: if (bytes && length >= 1) *bytes = 0xd8; return 1;
	case 0x0429: if (bytes && length >= 1) *bytes = 0xd9; return 1;
	case 0x042a: if (bytes && length >= 1) *bytes = 0xda; return 1;
	case 0x042b: if (bytes && length >= 1) *bytes = 0xdb; return 1;
	case 0x042c: if (bytes && length >= 1) *bytes = 0xdc; return 1;
	case 0x042d: if (bytes && length >= 1) *bytes = 0xdd; return 1;
	case 0x042e: if (bytes && length >= 1) *bytes = 0xde; return 1;
	case 0x042f: if (bytes && length >= 1) *bytes = 0xdf; return 1;
	case 0x0430: if (bytes && length >= 1) *bytes = 0xe0; return 1;
	case 0x0431: if (bytes && length >= 1) *bytes = 0xe1; return 1;
	case 0x0432: if (bytes && length >= 1) *bytes = 0xe2; return 1;
	case 0x0433: if (bytes && length >= 1) *bytes = 0xe3; return 1;
	case 0x0434: if (bytes && length >= 1) *bytes = 0xe4; return 1;
	case 0x0435: if (bytes && length >= 1) *bytes = 0xe5; return 1;
	case 0x0436: if (bytes && length >= 1) *bytes = 0xe6; return 1;
	case 0x0437: if (bytes && length >= 1) *bytes = 0xe7; return 1;
	case 0x0438: if (bytes && length >= 1) *bytes = 0xe8; return 1;
	case 0x0439: if (bytes && length >= 1) *bytes = 0xe9; return 1;
	case 0x043a: if (bytes && length >= 1) *bytes = 0xea; return 1;
	case 0x043b: if (bytes && length >= 1) *bytes = 0xeb; return 1;
	case 0x043c: if (bytes && length >= 1) *bytes = 0xec; return 1;
	case 0x043d: if (bytes && length >= 1) *bytes = 0xed; return 1;
	case 0x043e: if (bytes && length >= 1) *bytes = 0xee; return 1;
	case 0x043f: if (bytes && length >= 1) *bytes = 0xef; return 1;
	case 0x0440: if (bytes && length >= 1) *bytes = 0xf0; return 1;
	case 0x0441: if (bytes && length >= 1) *bytes = 0xf1; return 1;
	case 0x0442: if (bytes && length >= 1) *bytes = 0xf2; return 1;
	case 0x0443: if (bytes && length >= 1) *bytes = 0xf3; return 1;
	case 0x0444: if (bytes && length >= 1) *bytes = 0xf4; return 1;
	case 0x0445: if (bytes && length >= 1) *bytes = 0xf5; return 1;
	case 0x0446: if (bytes && length >= 1) *bytes = 0xf6; return 1;
	case 0x0447: if (bytes && length >= 1) *bytes = 0xf7; return 1;
	case 0x0448: if (bytes && length >= 1) *bytes = 0xf8; return 1;
	case 0x0449: if (bytes && length >= 1) *bytes = 0xf9; return 1;
	case 0x044a: if (bytes && length >= 1) *bytes = 0xfa; return 1;
	case 0x044b: if (bytes && length >= 1) *bytes = 0xfb; return 1;
	case 0x044c: if (bytes && length >= 1) *bytes = 0xfc; return 1;
	case 0x044d: if (bytes && length >= 1) *bytes = 0xfd; return 1;
	case 0x044e: if (bytes && length >= 1) *bytes = 0xfe; return 1;
	case 0x044f: if (bytes && length >= 1) *bytes = 0xff; return 1;
	default: return 0;
	}
}


int Windows1251Encoding::queryConvert(const unsigned char* bytes, int length) const
{
	if (1 <= length)
		return _charMap[*bytes];
	else
		return -1;
}


int Windows1251Encoding::sequenceLength(const unsigned char* bytes, int length) const
{
	return 1;
}


} // namespace Poco

