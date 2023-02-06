//
// AttributesParser.h
//
// Library: CppParser
// Package: Attributes
// Module:  AttributesParser
//
// Definition of the AttributesParser class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CppParser_AttributesParser_INCLUDED
#define CppParser_AttributesParser_INCLUDED


#include "Poco/CppParser/CppParser.h"
#include "Poco/CppParser/Tokenizer.h"
#include "Poco/CppParser/Attributes.h"


namespace Poco {
namespace CppParser {


class CppParser_API AttributesParser
	/// A parser for POCO-style C++ attributes.
	///
	/// Using a special comment syntax, C++ declarations for
	/// structs/classes, functions, types, etc. can be annotated
	/// with attributes.
	///
	/// Attributes always come immediately before the symbol that 
	/// is being annotated, and are written inside special comments
	/// with the syntax:
	///     //@ <attrDecl>[,<attrDec>...]
	/// where <attrDecl> is
	///     <name>[=<value>]
	/// <name> is a valid C++ identifier, or two identifiers separated by 
	/// a period (struct accessor notation).
	/// <value> is a string, integer, identifier, bool literal, or a complex value
	/// in the form
	///    {<name>=<value>[,<name>=<value>...]}
{
public:
	AttributesParser(Attributes& attrs, std::istream& istr);
		/// Creates the AttributesParser.

	~AttributesParser();
		/// Destroys the AttributesParser.

	void parse();
		/// Parses attributes.

protected:
	void setAttribute(const std::string& name, const std::string& value);
	const Poco::Token* parseAttributes(const Poco::Token* pNext);
	const Poco::Token* parseAttribute(const Poco::Token* pNext);
	const Poco::Token* parseComplexAttribute(const Token* pNext, const std::string& id);
	const Poco::Token* parseIdentifier(const Poco::Token* pNext, std::string& id);
	const Poco::Token* next();
	static bool isIdentifier(const Poco::Token* pToken);
	static bool isOperator(const Poco::Token* pToken, int kind);
	static bool isLiteral(const Poco::Token* pToken);
	static bool isEOF(const Poco::Token* pToken);
	
private:
	Attributes& _attrs;
	Tokenizer   _tokenizer;
	std::string _id;
};


//
// inlines
//
inline const Poco::Token* AttributesParser::next()
{
	return _tokenizer.next();
}


inline bool AttributesParser::isEOF(const Poco::Token* pToken)
{
	return pToken->is(Token::EOF_TOKEN);
}


inline bool AttributesParser::isIdentifier(const Poco::Token* pToken)
{
	return pToken->is(Poco::Token::IDENTIFIER_TOKEN) || pToken->is(Poco::Token::KEYWORD_TOKEN);
}


inline bool AttributesParser::isOperator(const Poco::Token* pToken, int kind)
{
	return pToken->is(Poco::Token::OPERATOR_TOKEN) && pToken->asInteger() == kind;
}


inline bool AttributesParser::isLiteral(const Poco::Token* pToken)
{
	return pToken->is(Poco::Token::STRING_LITERAL_TOKEN) || pToken->is(Poco::Token::INTEGER_LITERAL_TOKEN);
}


} } // namespace Poco::CppParser


#endif // CppParser_AttributesParser_INCLUDED
