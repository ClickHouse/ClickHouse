//
// NumberParser.cpp
//
// $Id: //poco/1.4/Foundation/src/NumberParser.cpp#4 $
//
// Library: Foundation
// Package: Core
// Module:  NumberParser
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/NumberParser.h"
#include "Poco/Exception.h"
#include "Poco/String.h"
#include "Poco/NumericString.h"
#include <cstdio>
#include <cctype>
#include <stdlib.h>
#if !defined(POCO_NO_LOCALE)
	#include <locale>
#endif


#if defined(POCO_LONG_IS_64_BIT)
	#define I64_FMT "l"
#elif defined(_MSC_VER) || defined(__MINGW32__)
	#define I64_FMT "I64"
#elif defined(__APPLE__) 
	#define I64_FMT "q"
#else
	#define I64_FMT "ll"
#endif


namespace Poco {


int NumberParser::parse(const std::string& s, char thSep)
{
	int result;
	if (tryParse(s, result, thSep))
		return result;
	else
		throw SyntaxException("Not a valid integer", s);
}


bool NumberParser::tryParse(const std::string& s, int& value, char thSep)
{
	return strToInt(s.c_str(), value, NUM_BASE_DEC, thSep);
}


unsigned NumberParser::parseUnsigned(const std::string& s, char thSep)
{
	unsigned result;
	if (tryParseUnsigned(s, result, thSep))
		return result;
	else
		throw SyntaxException("Not a valid unsigned integer", s);
}


bool NumberParser::tryParseUnsigned(const std::string& s, unsigned& value, char thSep)
{
	return strToInt(s.c_str(), value, NUM_BASE_DEC, thSep);
}


unsigned NumberParser::parseHex(const std::string& s)
{
	unsigned result;
	if (tryParseHex(s, result))
		return result;
	else
		throw SyntaxException("Not a valid hexadecimal integer", s);
}


bool NumberParser::tryParseHex(const std::string& s, unsigned& value)
{
	return strToInt(s.c_str(), value, NUM_BASE_HEX);
}


unsigned NumberParser::parseOct(const std::string& s)
{
	unsigned result;
	if (tryParseOct(s, result))
		return result;
	else
		throw SyntaxException("Not a valid hexadecimal integer", s);
}


bool NumberParser::tryParseOct(const std::string& s, unsigned& value)
{
	return strToInt(s.c_str(), value, NUM_BASE_OCT);
}


#if defined(POCO_HAVE_INT64)


Int64 NumberParser::parse64(const std::string& s, char thSep)
{
	Int64 result;
	if (tryParse64(s, result, thSep))
		return result;
	else
		throw SyntaxException("Not a valid integer", s);
}


bool NumberParser::tryParse64(const std::string& s, Int64& value, char thSep)
{
	return strToInt(s.c_str(), value, NUM_BASE_DEC, thSep);
}


UInt64 NumberParser::parseUnsigned64(const std::string& s, char thSep)
{
	UInt64 result;
	if (tryParseUnsigned64(s, result, thSep))
		return result;
	else
		throw SyntaxException("Not a valid unsigned integer", s);
}


bool NumberParser::tryParseUnsigned64(const std::string& s, UInt64& value, char thSep)
{
	return strToInt(s.c_str(), value, NUM_BASE_DEC, thSep);
}


UInt64 NumberParser::parseHex64(const std::string& s)
{
	UInt64 result;
	if (tryParseHex64(s, result))
		return result;
	else
		throw SyntaxException("Not a valid hexadecimal integer", s);
}


bool NumberParser::tryParseHex64(const std::string& s, UInt64& value)
{
	return strToInt(s.c_str(), value, NUM_BASE_HEX);
}


UInt64 NumberParser::parseOct64(const std::string& s)
{
	UInt64 result;
	if (tryParseOct64(s, result))
		return result;
	else
		throw SyntaxException("Not a valid hexadecimal integer", s);
}


bool NumberParser::tryParseOct64(const std::string& s, UInt64& value)
{
	return strToInt(s.c_str(), value, NUM_BASE_OCT);
}


#endif // defined(POCO_HAVE_INT64)


double NumberParser::parseFloat(const std::string& s, char decSep, char thSep)
{
	double result;
	if (tryParseFloat(s, result, decSep, thSep))
		return result;
	else
		throw SyntaxException("Not a valid floating-point number", s);
}


bool NumberParser::tryParseFloat(const std::string& s, double& value, char decSep, char thSep)
{
	return strToDouble(s.c_str(), value, decSep, thSep);
}


bool NumberParser::parseBool(const std::string& s)
{
	bool result;
	if (tryParseBool(s, result))
		return result;
	else
		throw SyntaxException("Not a valid bool number", s);
}

	
bool NumberParser::tryParseBool(const std::string& s, bool& value)
{
	int n;
	if (NumberParser::tryParse(s, n))
	{
		value = (n != 0);
		return true;
	}

	if (icompare(s, "true") == 0)
	{
		value = true;
		return true;
	}
	else if (icompare(s, "yes") == 0)
	{
		value = true;
		return true;
	}
	else if (icompare(s, "on") == 0)
	{
		value = true;
		return true;
	}
	
	if (icompare(s, "false") == 0)
	{
		value = false;
		return true;
	}
	else if (icompare(s, "no") == 0)
	{
		value = false;
		return true;
	}
	else if (icompare(s, "off") == 0)
	{
		value = false;
		return true;
	}
	
	return false;
}


} // namespace Poco
