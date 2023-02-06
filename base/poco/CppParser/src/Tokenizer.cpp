//
// Tokenizer.cpp
//
// Library: CppParser
// Package: CppParser
// Module:  Tokenizer
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/CppParser/Tokenizer.h"
#include "Poco/CppParser/CppToken.h"


using Poco::StreamTokenizer;
using Poco::WhitespaceToken;


namespace Poco {
namespace CppParser {


Tokenizer::Tokenizer(std::istream& istr):
	StreamTokenizer(istr)	
{
	addToken(new OperatorToken);
	addToken(new IdentifierToken);
	addToken(new StringLiteralToken);
	addToken(new CharLiteralToken);
	addToken(new NumberLiteralToken);
	addToken(new CommentToken, false);
	addToken(new PreprocessorToken);
	addToken(new WhitespaceToken);
}


Tokenizer::~Tokenizer()
{
}


} } // namespace Poco::CppParser
