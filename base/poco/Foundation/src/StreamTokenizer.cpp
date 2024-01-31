//
// StreamTokenizer.cpp
//
// Library: Foundation
// Package: Streams
// Module:  StreamTokenizer
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/StreamTokenizer.h"


namespace Poco {


StreamTokenizer::StreamTokenizer():
	_pIstr(0)
{
}


StreamTokenizer::StreamTokenizer(std::istream& istr):
	_pIstr(&istr)
{
}


StreamTokenizer::~StreamTokenizer()
{
	for (TokenVec::iterator it = _tokens.begin(); it != _tokens.end(); ++it)
	{
		delete it->pToken;
	}
}


void StreamTokenizer::attachToStream(std::istream& istr)
{
	_pIstr = &istr;
}


void StreamTokenizer::addToken(Token* pToken)
{
	poco_check_ptr (pToken);

	TokenInfo ti;
	ti.pToken = pToken;
	ti.ignore = (pToken->tokenClass() == Token::COMMENT_TOKEN || pToken->tokenClass() == Token::WHITESPACE_TOKEN);
	_tokens.push_back(ti);
}


void StreamTokenizer::addToken(Token* pToken, bool ignore)
{
	poco_check_ptr (pToken);

	TokenInfo ti;
	ti.pToken = pToken;
	ti.ignore = ignore;
	_tokens.push_back(ti);
}

	
const Token* StreamTokenizer::next()
{
	poco_check_ptr (_pIstr);
	
	static const int eof = std::char_traits<char>::eof();

	int first = _pIstr->get();
	TokenVec::const_iterator it = _tokens.begin();
	while (first != eof && it != _tokens.end())
	{
		const TokenInfo& ti = *it;
		if (ti.pToken->start((char) first, *_pIstr))
		{
			ti.pToken->finish(*_pIstr);
			if (ti.ignore) 
			{
				first = _pIstr->get();
				it = _tokens.begin();
			}
			else return ti.pToken;
		}
		else ++it;
	}
	if (first == eof)
	{
		return &_eofToken;
	}
	else
	{
		_invalidToken.start((char) first, *_pIstr);
		return &_invalidToken;
	}
}


} // namespace Poco
