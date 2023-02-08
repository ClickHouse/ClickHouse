//
// UnicodeConverter.cpp
//
// Library: Foundation
// Package: Text
// Module:  UnicodeConverter
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/UnicodeConverter.h"
#include "Poco/TextConverter.h"
#include "Poco/TextIterator.h"
#include "Poco/UTF8Encoding.h"
#include "Poco/UTF16Encoding.h"
#include "Poco/UTF32Encoding.h"
#include <cstring>


namespace Poco {


void UnicodeConverter::convert(const std::string& utf8String, UTF32String& utf32String)
{
	utf32String.clear();
	UTF8Encoding utf8Encoding;
	TextIterator it(utf8String, utf8Encoding);
	TextIterator end(utf8String);

	while (it != end)
	{
		int cc = *it++;
		utf32String += (UTF32Char) cc;
	}
}


void UnicodeConverter::convert(const char* utf8String, std::size_t length, UTF32String& utf32String)
{
	if (!utf8String || !length)
	{
		utf32String.clear();
		return;
	}

	convert(std::string(utf8String, utf8String + length), utf32String);
}


void UnicodeConverter::convert(const char* utf8String, UTF32String& utf32String)
{
	if (!utf8String || !std::strlen(utf8String))
	{
		utf32String.clear();
		return;
	}

	convert(utf8String, std::strlen(utf8String), utf32String);
}


void UnicodeConverter::convert(const std::string& utf8String, UTF16String& utf16String)
{
	utf16String.clear();
	UTF8Encoding utf8Encoding;
	TextIterator it(utf8String, utf8Encoding);
	TextIterator end(utf8String);
	while (it != end) 
	{
		int cc = *it++;
		if (cc <= 0xffff)
		{
			utf16String += (UTF16Char) cc;
		}
		else
		{
			cc -= 0x10000;
			utf16String += (UTF16Char) ((cc >> 10) & 0x3ff) | 0xd800;
			utf16String += (UTF16Char) (cc & 0x3ff) | 0xdc00;
		}
	}
}


void UnicodeConverter::convert(const char* utf8String,  std::size_t length, UTF16String& utf16String)
{
	if (!utf8String || !length)
	{
		utf16String.clear();
		return;
	}

	convert(std::string(utf8String, utf8String + length), utf16String);
}


void UnicodeConverter::convert(const char* utf8String, UTF16String& utf16String)
{
	if (!utf8String || !std::strlen(utf8String))
	{
		utf16String.clear();
		return;
	}

	convert(std::string(utf8String), utf16String);
}


void UnicodeConverter::convert(const UTF16String& utf16String, std::string& utf8String)
{
	utf8String.clear();
	UTF8Encoding utf8Encoding;
	UTF16Encoding utf16Encoding;
	TextConverter converter(utf16Encoding, utf8Encoding);
	converter.convert(utf16String.data(), (int) utf16String.length() * sizeof(UTF16Char), utf8String);
}


void UnicodeConverter::convert(const UTF32String& utf32String, std::string& utf8String)
{
	utf8String.clear();
	UTF8Encoding utf8Encoding;
	UTF32Encoding utf32Encoding;
	TextConverter converter(utf32Encoding, utf8Encoding);
	converter.convert(utf32String.data(), (int) utf32String.length() * sizeof(UTF32Char), utf8String);
}


void UnicodeConverter::convert(const UTF16Char* utf16String,  std::size_t length, std::string& utf8String)
{
	utf8String.clear();
	UTF8Encoding utf8Encoding;
	UTF16Encoding utf16Encoding;
	TextConverter converter(utf16Encoding, utf8Encoding);
	converter.convert(utf16String, (int) length * sizeof(UTF16Char), utf8String);
}


void UnicodeConverter::convert(const UTF32Char* utf32String,  std::size_t length, std::string& utf8String)
{
	toUTF8(UTF32String(utf32String, length), utf8String);
}


void UnicodeConverter::convert(const UTF16Char* utf16String, std::string& utf8String)
{
	toUTF8(utf16String, UTFStrlen(utf16String), utf8String);
}


void UnicodeConverter::convert(const UTF32Char* utf32String, std::string& utf8String)
{
	toUTF8(utf32String, UTFStrlen(utf32String), utf8String);
}


} // namespace Poco
