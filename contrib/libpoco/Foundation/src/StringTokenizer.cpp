//
// StringTokenizer.cpp
//
// $Id: //poco/1.4/Foundation/src/StringTokenizer.cpp#1 $
//
// Library: Foundation
// Package: Core
// Module:	StringTokenizer
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/StringTokenizer.h"
#include "Poco/Ascii.h"

#include <algorithm>

namespace Poco {


StringTokenizer::StringTokenizer(const std::string& str, const std::string& separators, int options)
{
	std::string::const_iterator it = str.begin();
	std::string::const_iterator end = str.end();
	std::string token;
	bool doTrim = ((options & TOK_TRIM) != 0);
	bool ignoreEmpty = ((options & TOK_IGNORE_EMPTY) != 0);
	bool lastToken = false;

	for (;it != end; ++it)
	{
		if (separators.find(*it) != std::string::npos) 
		{
			if (doTrim) trim(token);
			if (!token.empty() || !ignoreEmpty)_tokens.push_back(token);
			if (!ignoreEmpty) lastToken = true;
			token = "";
		}
		else
		{
			token += *it;
			lastToken = false;
		}
	}

	if (!token.empty())
	{
		if (doTrim) trim(token);
		if (!token.empty()) _tokens.push_back(token);
	}
	else if (lastToken) _tokens.push_back("");
}


StringTokenizer::~StringTokenizer()
{
}


void StringTokenizer::trim (std::string& token)
{
	std::size_t front = 0, back = 0, length = token.length();
	std::string::const_iterator tIt = token.begin();
	std::string::const_iterator tEnd = token.end();
	for (; tIt != tEnd; ++tIt, ++front)
	{
		if (!Ascii::isSpace(*tIt)) break;
	}
	if (tIt != tEnd)
	{
		std::string::const_reverse_iterator tRit = token.rbegin();
		std::string::const_reverse_iterator tRend = token.rend();
		for (; tRit != tRend; ++tRit, ++back)
		{
			if (!Ascii::isSpace(*tRit)) break;
		}
	}
	token = token.substr(front, length - back - front);
}


std::size_t StringTokenizer::count(const std::string& token) const
{
	std::size_t result = 0;
	TokenVec::const_iterator it = std::find(_tokens.begin(), _tokens.end(), token);
	while(it != _tokens.end())
	{
		result++;
		it = std::find(++it, _tokens.end(), token);
	}
	return result;
}


std::size_t StringTokenizer::find(const std::string& token, std::size_t pos) const
{	
	TokenVec::const_iterator it = std::find(_tokens.begin() + pos, _tokens.end(), token);
	if ( it != _tokens.end() )
	{
		return it - _tokens.begin();
	}
	throw NotFoundException(token);
}

bool StringTokenizer::has(const std::string& token) const
{
	TokenVec::const_iterator it = std::find(_tokens.begin(), _tokens.end(), token);
	return it != _tokens.end();
}

std::size_t StringTokenizer::replace(const std::string& oldToken, const std::string& newToken, std::size_t pos)
{
	std::size_t result = 0;
	TokenVec::iterator it = std::find(_tokens.begin() + pos, _tokens.end(), oldToken);
	while(it != _tokens.end())
	{
		result++;
		*it = newToken;
		it = std::find(++it, _tokens.end(), oldToken);
	}
	return result;
}


} // namespace Poco

