//
// UTF8String.cpp
//
// Library: Foundation
// Package: Text
// Module:  UTF8String
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/UTF8String.h"
#include "Poco/Unicode.h"
#include "Poco/TextIterator.h"
#include "Poco/TextConverter.h"
#include "Poco/UTF8Encoding.h"
#include "Poco/NumberFormatter.h"
#include "Poco/Ascii.h"
#include <algorithm>


namespace Poco {


namespace
{
	static UTF8Encoding utf8;
}


int UTF8::icompare(const std::string& str, std::string::size_type pos, std::string::size_type n, std::string::const_iterator it2, std::string::const_iterator end2)
{	
	std::string::size_type sz = str.size();
	if (pos > sz) pos = sz;
	if (pos + n > sz) n = sz - pos;
	TextIterator uit1(str.begin() + pos, str.begin() + pos + n, utf8); 
	TextIterator uend1(str.begin() + pos + n);
	TextIterator uit2(it2, end2, utf8);
	TextIterator uend2(end2);
	while (uit1 != uend1 && uit2 != uend2)
	{
        int c1 = Unicode::toLower(*uit1);
        int c2 = Unicode::toLower(*uit2);
        if (c1 < c2)
            return -1;
        else if (c1 > c2)
            return 1;
        ++uit1; ++uit2;
	}
    
    if (uit1 == uend1)
		return uit2 == uend2 ? 0 : -1;
    else
        return 1;
}


int UTF8::icompare(const std::string& str1, const std::string& str2)
{
	return icompare(str1, 0, str1.size(), str2.begin(), str2.end());
}


int UTF8::icompare(const std::string& str1, std::string::size_type n1, const std::string& str2, std::string::size_type n2)
{
	if (n2 > str2.size()) n2 = str2.size();
	return icompare(str1, 0, n1, str2.begin(), str2.begin() + n2);
}


int UTF8::icompare(const std::string& str1, std::string::size_type n, const std::string& str2)
{
	if (n > str2.size()) n = str2.size();
	return icompare(str1, 0, n, str2.begin(), str2.begin() + n);
}


int UTF8::icompare(const std::string& str1, std::string::size_type pos, std::string::size_type n, const std::string& str2)
{
	return icompare(str1, pos, n, str2.begin(), str2.end());
}


int UTF8::icompare(const std::string& str1, std::string::size_type pos1, std::string::size_type n1, const std::string& str2, std::string::size_type pos2, std::string::size_type n2)
{
	std::string::size_type sz2 = str2.size();
	if (pos2 > sz2) pos2 = sz2;
	if (pos2 + n2 > sz2) n2 = sz2 - pos2;
	return icompare(str1, pos1, n1, str2.begin() + pos2, str2.begin() + pos2 + n2);
}


int UTF8::icompare(const std::string& str1, std::string::size_type pos1, std::string::size_type n, const std::string& str2, std::string::size_type pos2)
{
	std::string::size_type sz2 = str2.size();
	if (pos2 > sz2) pos2 = sz2;
	if (pos2 + n > sz2) n = sz2 - pos2;
	return icompare(str1, pos1, n, str2.begin() + pos2, str2.begin() + pos2 + n);
}


int UTF8::icompare(const std::string& str, std::string::size_type pos, std::string::size_type n, const std::string::value_type* ptr)
{
	poco_check_ptr (ptr);
	std::string str2(ptr); // TODO: optimize
	return icompare(str, pos, n, str2.begin(), str2.end());
}


int UTF8::icompare(const std::string& str, std::string::size_type pos, const std::string::value_type* ptr)
{
	return icompare(str, pos, str.size() - pos, ptr);
}


int UTF8::icompare(const std::string& str, const std::string::value_type* ptr)
{
	return icompare(str, 0, str.size(), ptr);
}


std::string UTF8::toUpper(const std::string& str)
{
	std::string result;
	TextConverter converter(utf8, utf8);
	converter.convert(str, result, Unicode::toUpper);
	return result;
}


std::string& UTF8::toUpperInPlace(std::string& str)
{
	std::string result;
	TextConverter converter(utf8, utf8);
	converter.convert(str, result, Unicode::toUpper);
	std::swap(str, result);
	return str;
}


std::string UTF8::toLower(const std::string& str)
{
	std::string result;
	TextConverter converter(utf8, utf8);
	converter.convert(str, result, Unicode::toLower);
	return result;
}


std::string& UTF8::toLowerInPlace(std::string& str)
{
	std::string result;
	TextConverter converter(utf8, utf8);
	converter.convert(str, result, Unicode::toLower);
	std::swap(str, result);
	return str;
}


void UTF8::removeBOM(std::string& str)
{
	if (str.size() >= 3 
		&& static_cast<unsigned char>(str[0]) == 0xEF 
		&& static_cast<unsigned char>(str[1]) == 0xBB 
		&& static_cast<unsigned char>(str[2]) == 0xBF)
	{
		str.erase(0, 3);
	}
}


std::string UTF8::escape(const std::string &s, bool strictJSON)
{
	return escape(s.begin(), s.end(), strictJSON);
}


std::string UTF8::escape(const std::string::const_iterator& begin, const std::string::const_iterator& end, bool strictJSON)
{
	static Poco::UInt32 offsetsFromUTF8[6] = {
		0x00000000UL, 0x00003080UL, 0x000E2080UL,
		0x03C82080UL, 0xFA082080UL, 0x82082080UL
	};

	std::string result;

	std::string::const_iterator it = begin;

	while(it != end)
	{
		Poco::UInt32 ch = 0;
		unsigned int sz = 0;

		do
		{
			ch <<= 6;
			ch += (unsigned char)*it++;
			sz++;
		}
		while (it != end && (*it & 0xC0) == 0x80 && sz < 6);
		ch -= offsetsFromUTF8[sz-1];

		if (ch == '\n') result += "\\n";
		else if (ch == '\t') result += "\\t";
		else if (ch == '\r') result += "\\r";
		else if (ch == '\b') result += "\\b";
		else if (ch == '\f') result += "\\f";
		else if (ch == '\v') result += (strictJSON ? "\\u000B" : "\\v");
		else if (ch == '\a') result += (strictJSON ? "\\u0007" : "\\a");
		else if (ch == '\\') result +=  "\\\\";
		else if (ch == '\"') result +=  "\\\"";
		else if (ch == '/') result +=  "\\/";
		else if (ch == '\0') result += "\\u0000";
		else if (ch < 32 || ch == 0x7f)
		{
			result += "\\u";
			NumberFormatter::appendHex(result, (unsigned short) ch, 4);
		}
		else if (ch > 0xFFFF)
		{
			ch -= 0x10000;
			result += "\\u";
			NumberFormatter::appendHex(result, (unsigned short) (( ch >> 10 ) & 0x03ff ) + 0xd800, 4);
			result += "\\u";
			NumberFormatter::appendHex(result, (unsigned short) (ch & 0x03ff ) + 0xdc00, 4);
		}
		else if (ch >= 0x80 && ch <= 0xFFFF)
		{
			result += "\\u";
			NumberFormatter::appendHex(result, (unsigned short) ch, 4);
		}
		else
		{
			result += (char) ch;
		}
	}
	return result;
}


std::string UTF8::unescape(const std::string &s)
{
	return unescape(s.begin(), s.end());
}


std::string UTF8::unescape(const std::string::const_iterator& begin, const std::string::const_iterator& end)
{
	std::string result;

	std::string::const_iterator it = begin;

	while (it != end)
	{
		Poco::UInt32 ch = (Poco::UInt32) *it++;

		if (ch == '\\')
		{
			if ( it == end )
			{
				//Invalid sequence!
			}

			if (*it == 'n')
			{
				ch = '\n';
				it++;
			}
			else if (*it == 't')
			{
				ch = '\t';
				it++;
			}
			else if (*it == 'r')
			{
				ch = '\r';
				it++;
			}
			else if (*it == 'b')
			{
				ch = '\b';
				it++;
			}
			else if (*it == 'f')
			{
				ch = '\f';
				it++;
			}
			else if (*it == 'v')
			{
				ch = '\v';
				it++;
			}
			else if (*it == 'a')
			{
				ch = '\a';
				it++;
			}
			else if (*it == 'u')
			{
				char digs[5];
				std::memset(digs, 0, 5);
				unsigned int dno = 0;

				it++;

				while (it != end && Ascii::isHexDigit(*it) && dno < 4) digs[dno++] = *it++;
				if (dno > 0)
				{
					ch = std::strtol(digs, NULL, 16);
				}

				if( ch >= 0xD800 && ch <= 0xDBFF )
				{
					if ( it == end || *it != '\\' )
					{
						//Invalid sequence!
					}
					else
					{
						it++;
						if ( it == end || *it != 'u' )
						{
							//Invalid sequence!
						}
						else
						{
							it++;
						}
					}

					// UTF-16 surrogate pair. Go fetch other half
					std::memset(digs, 0, 5);
					dno = 0;
					while (it != end && Ascii::isHexDigit(*it) && dno < 4) digs[dno++] = *it++;
					if (dno > 0)
					{
						Poco::UInt32 temp = std::strtol(digs, NULL, 16);
						if( temp >= 0xDC00 && temp <= 0xDFFF )
						{
							ch = ( ( ( ch - 0xD800 ) << 10 ) | ( temp - 0xDC00 ) ) + 0x10000;
						}
					}
				}
			}
			else if (*it == 'U')
			{
				char digs[9];
				std::memset(digs, 0, 9);
				unsigned int dno = 0;

				it++;
				while (it != end && Ascii::isHexDigit(*it) && dno < 8)
				{
					digs[dno++] = *it++;
				}
				if (dno > 0)
				{
					ch = std::strtol(digs, NULL, 16);
				}
			}
		}

		unsigned char utf8[4];
		UTF8Encoding encoding;
		int sz = encoding.convert(ch, utf8, 4);
		result.append((char*) utf8, sz);
	}

	return result;
}


} // namespace Poco
