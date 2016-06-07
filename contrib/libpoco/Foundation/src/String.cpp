//
// String.h
//
// $Id: //poco/1.4/Foundation/src/String.cpp#1 $
//
// Library: Foundation
// Package: Core
// Module:  String
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/String.h"


namespace Poco {


#if defined(POCO_NO_TEMPLATE_ICOMPARE)


int icompare(const std::string& str, std::string::size_type pos, std::string::size_type n, std::string::const_iterator it2, std::string::const_iterator end2)
{
	std::string::size_type sz = str.size();
	if (pos > sz) pos = sz;
	if (pos + n > sz) n = sz - pos;
	std::string::const_iterator it1  = str.begin() + pos; 
	std::string::const_iterator end1 = str.begin() + pos + n;
	while (it1 != end1 && it2 != end2)
	{
	std::string::value_type c1 = Ascii::toLower(*it1);
	std::string::value_type c2 = Ascii::toLower(*it2);
	if (c1 < c2)
		return -1;
	else if (c1 > c2)
		return 1;
	++it1; ++it2;
	}

	if (it1 == end1)
		return it2 == end2 ? 0 : -1;
	else
	return 1;
}


int icompare(const std::string& str1, const std::string& str2)
{
	return icompare(str1, 0, str1.size(), str2.begin(), str2.end());
}


int icompare(const std::string& str1, std::string::size_type n1, const std::string& str2, std::string::size_type n2)
{
	if (n2 > str2.size()) n2 = str2.size();
	return icompare(str1, 0, n1, str2.begin(), str2.begin() + n2);
}


int icompare(const std::string& str1, std::string::size_type n, const std::string& str2)
{
	if (n > str2.size()) n = str2.size();
	return icompare(str1, 0, n, str2.begin(), str2.begin() + n);
}


int icompare(const std::string& str1, std::string::size_type pos, std::string::size_type n, const std::string& str2)
{
	return icompare(str1, pos, n, str2.begin(), str2.end());
}


int icompare(const std::string& str1, std::string::size_type pos1, std::string::size_type n1, const std::string& str2, std::string::size_type pos2, std::string::size_type n2)
{
	std::string::size_type sz2 = str2.size();
	if (pos2 > sz2) pos2 = sz2;
	if (pos2 + n2 > sz2) n2 = sz2 - pos2;
	return icompare(str1, pos1, n1, str2.begin() + pos2, str2.begin() + pos2 + n2);
}


int icompare(const std::string& str1, std::string::size_type pos1, std::string::size_type n, const std::string& str2, std::string::size_type pos2)
{
	std::string::size_type sz2 = str2.size();
	if (pos2 > sz2) pos2 = sz2;
	if (pos2 + n > sz2) n = sz2 - pos2;
	return icompare(str1, pos1, n, str2.begin() + pos2, str2.begin() + pos2 + n);
}


int icompare(const std::string& str, std::string::size_type pos, std::string::size_type n, const std::string::value_type* ptr)
{
	poco_check_ptr (ptr);
	std::string::size_type sz = str.size();
	if (pos > sz) pos = sz;
	if (pos + n > sz) n = sz - pos;
	std::string::const_iterator it  = str.begin() + pos; 
	std::string::const_iterator end = str.begin() + pos + n;
	while (it != end && *ptr)
	{
	std::string::value_type c1 = Ascii::toLower(*it);
	std::string::value_type c2 = Ascii::toLower(*ptr);
	if (c1 < c2)
		return -1;
	else if (c1 > c2)
		return 1;
	++it; ++ptr;
	}

	if (it == end)
		return *ptr == 0 ? 0 : -1;
	else
	return 1;
}


int icompare(const std::string& str, std::string::size_type pos, const std::string::value_type* ptr)
{
	return icompare(str, pos, str.size() - pos, ptr);
}


int icompare(const std::string& str, const std::string::value_type* ptr)
{
	return icompare(str, 0, str.size(), ptr);
}


std::string replace(const std::string& str, const std::string& from, const std::string& to, std::string::size_type start)
{
	std::string result(str);
	replaceInPlace(result, from, to, start);
	return result;
}


std::string replace(const std::string& str, const std::string::value_type* from, const std::string::value_type* to, std::string::size_type start)
{
	std::string result(str);
	replaceInPlace(result, from, to, start);
	return result;
}


std::string replace(const std::string& str, const std::string::value_type from, const std::string::value_type to, std::string::size_type start)
{
	std::string result(str);
	replaceInPlace(result, from, to, start);
	return result;
}


std::string remove(const std::string& str, const std::string::value_type ch, std::string::size_type start)
{
	std::string result(str);
	replaceInPlace(result, ch, 0, start);
	return result;
}

	
std::string& replaceInPlace(std::string& str, const std::string& from, const std::string& to, std::string::size_type start)
{
	poco_assert (from.size() > 0);
	
	std::string result;
	std::string::size_type pos = 0;
	result.append(str, 0, start);
	do
	{
		pos = str.find(from, start);
		if (pos != std::string::npos)
		{
			result.append(str, start, pos - start);
			result.append(to);
			start = pos + from.length();
		}
		else result.append(str, start, str.size() - start);
	}
	while (pos != std::string::npos);
	str.swap(result);
	return str;
}


std::string& replaceInPlace(std::string& str, const std::string::value_type* from, const std::string::value_type* to, std::string::size_type start)
{
	poco_assert (*from);

	std::string result;
	std::string::size_type pos = 0;
	std::string::size_type fromLen = std::strlen(from);
	result.append(str, 0, start);
	do
	{
		pos = str.find(from, start);
		if (pos != std::string::npos)
		{
			result.append(str, start, pos - start);
			result.append(to);
			start = pos + fromLen;
		}
		else result.append(str, start, str.size() - start);
	}
	while (pos != std::string::npos);
	str.swap(result);
	return str;
}


std::string& replaceInPlace(std::string& str, const std::string::value_type from, const std::string::value_type to, std::string::size_type start)
{
	if (from == to) return str;

	std::string::size_type pos = 0;
	do
	{
		pos = str.find(from, start);
		if (pos != std::string::npos)
		{
			if (to) str[pos] = to;
			else str.erase(pos, 1);
		}
	} while (pos != std::string::npos);

	return str;
}


std::string& removeInPlace(std::string& str, const std::string::value_type ch, std::string::size_type start)
{
	return replaceInPlace(str, ch, 0, start);
}


#endif


} // namespace Poco
