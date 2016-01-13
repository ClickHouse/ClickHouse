//
// Token.cpp
//
// $Id: //poco/1.4/Foundation/src/Token.cpp#1 $
//
// Library: Foundation
// Package: Streams
// Module:  StringTokenizer
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Token.h"
#include "Poco/NumberParser.h"
#include "Poco/Ascii.h"


namespace Poco {


Token::Token()
{
}


Token::~Token()
{
}


bool Token::start(char c, std::istream& istr)
{
	_value = c;
	return false;
}


void Token::finish(std::istream& istr)
{
}


Token::Class Token::tokenClass() const
{
	return INVALID_TOKEN;
}

		
std::string Token::asString() const
{
	return _value;
}


#if defined(POCO_HAVE_INT64)
Int64 Token::asInteger64() const
{
	return NumberParser::parse64(_value);
}


UInt64 Token::asUnsignedInteger64() const
{
	return NumberParser::parseUnsigned64(_value);
}
#endif


int Token::asInteger() const
{
	return NumberParser::parse(_value);
}


unsigned Token::asUnsignedInteger() const
{
	return NumberParser::parseUnsigned(_value);
}


double Token::asFloat() const
{
	return NumberParser::parseFloat(_value);
}


char Token::asChar() const
{
	return _value.empty() ? 0 : _value[0];
}


InvalidToken::InvalidToken()
{
}


InvalidToken::~InvalidToken()
{
}


Token::Class InvalidToken::tokenClass() const
{
	return INVALID_TOKEN;
}


EOFToken::EOFToken()
{
}


EOFToken::~EOFToken()
{
}


Token::Class EOFToken::tokenClass() const
{
	return EOF_TOKEN;
}


WhitespaceToken::WhitespaceToken()
{
}


WhitespaceToken::~WhitespaceToken()
{
}


Token::Class WhitespaceToken::tokenClass() const
{
	return WHITESPACE_TOKEN;
}


bool WhitespaceToken::start(char c, std::istream& istr)
{
	if (Ascii::isSpace(c))
	{
		_value = c;
		return true;
	}
	return false;
}


void WhitespaceToken::finish(std::istream& istr)
{
	int c = istr.peek();
	while (Ascii::isSpace(c))
	{
		istr.get();
		_value += (char) c;
		c = istr.peek();
	}
}


} // namespace Poco
