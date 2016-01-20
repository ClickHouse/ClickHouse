//
// UTF8String.cpp
//
// $Id: //poco/1.4/Foundation/src/UTF8String.cpp#2 $
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


} // namespace Poco
