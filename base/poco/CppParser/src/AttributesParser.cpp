//
// AttributesParser.cpp
//
// Library: CppParser
// Package: Attributes
// Module:  AttributesParser
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/CppParser/AttributesParser.h"
#include "Poco/CppParser/CppToken.h"
#include "Poco/Exception.h"


using Poco::Token;
using Poco::SyntaxException;


namespace Poco {
namespace CppParser {


AttributesParser::AttributesParser(Attributes& attrs, std::istream& istr):
	_attrs(attrs),
	_tokenizer(istr)
{
}


AttributesParser::~AttributesParser()
{
}


void AttributesParser::parse()
{
	const Token* pNext = next();
	if (!isEOF(pNext))
	{
		pNext = parseAttributes(pNext);
	}
	if (!isEOF(pNext))
		throw Poco::SyntaxException("extra tokens found in attribute declaration");
}


const Token* AttributesParser::parseAttributes(const Token* pNext)
{
	pNext = parseAttribute(pNext);
	while (isOperator(pNext, OperatorToken::OP_COMMA) || isIdentifier(pNext))
	{
		if (!isIdentifier(pNext)) pNext = next();
		pNext = parseAttribute(pNext);
	}
	return pNext;
}


const Token* AttributesParser::parseAttribute(const Token* pNext)
{
	std::string id;
	std::string value;
	pNext = parseIdentifier(pNext, id);
	if (isOperator(pNext, OperatorToken::OP_ASSIGN))
	{
		pNext = next();
		if (isOperator(pNext, OperatorToken::OP_OPENBRACE))
		{
			pNext = parseComplexAttribute(pNext, id);
		}
		else if (isIdentifier(pNext) || isLiteral(pNext))
		{
			value = pNext->asString();
			pNext = next();
		}
		else throw SyntaxException("bad attribute declaration");
	}
	setAttribute(id, value);
	return pNext;
}


const Token* AttributesParser::parseComplexAttribute(const Token* pNext, const std::string& id)
{
	poco_assert_dbg (isOperator(pNext, OperatorToken::OP_OPENBRACE));
	
	pNext = next();
	std::string oldId(_id);
	if (!_id.empty())
	{
		_id.append(".");
		_id.append(id);
	}
	else _id = id;
	pNext = parseAttributes(pNext);
	_id = oldId;
	if (isOperator(pNext, OperatorToken::OP_CLOSBRACE))
		pNext = next();
	else
		throw SyntaxException("bad attribute declaration");
	
	return pNext;
}


const Token* AttributesParser::parseIdentifier(const Token* pNext, std::string& id)
{
	if (isIdentifier(pNext))
	{
		id = pNext->asString();
		pNext = next();
		while (isOperator(pNext, OperatorToken::OP_PERIOD))
		{
			id.append(".");
			pNext = next();
			if (isIdentifier(pNext))
			{
				id.append(pNext->asString());
				pNext = next();
			}
			else throw SyntaxException("identifier expected");
		}
		return pNext;
	}
	else throw SyntaxException("identifier expected");
}


void AttributesParser::setAttribute(const std::string& name, const std::string& value)
{
	std::string n;
	if (!_id.empty())
	{
		n.append(_id);
		n.append(".");
	}
	n.append(name);
	_attrs.set(n, value);
}


} } // namespace Poco::CppParser
