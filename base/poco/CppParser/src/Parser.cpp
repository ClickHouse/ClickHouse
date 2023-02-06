//
// Parser.cpp
//
// Library: CppParser
// Package: CppParser
// Module:  Parser
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/CppParser/Parser.h"
#include "Poco/CppParser/CppToken.h"
#include "Poco/CppParser/Decl.h"
#include "Poco/CppParser/Enum.h"
#include "Poco/CppParser/EnumValue.h"
#include "Poco/CppParser/Function.h"
#include "Poco/CppParser/NameSpace.h"
#include "Poco/CppParser/Parameter.h"
#include "Poco/CppParser/Struct.h"
#include "Poco/CppParser/TypeDef.h"
#include "Poco/CppParser/Variable.h"
#include "Poco/CppParser/AttributesParser.h"
#include "Poco/Path.h"
#include "Poco/String.h"
#include "Poco/NumberFormatter.h"
#include "Poco/Exception.h"
#include <sstream>
#include <cctype>


using Poco::Token;
using Poco::WhitespaceToken;
using Poco::Path;
using Poco::NumberFormatter;
using Poco::SyntaxException;
using Poco::icompare;
using Poco::trimInPlace;


namespace Poco {
namespace CppParser {


Parser::Parser(NameSpace::SymbolTable& gst, const std::string& file, std::istream& istr):
	_gst(gst),
	_istr(istr),
	_tokenizer(_istr),
	_file(file),
	_inFile(false),
	_pCurrentSymbol(0),
	_access(Symbol::ACC_PUBLIC)
{
	Path p(file);
	p.makeAbsolute();
	_path = p.toString();
	_currentPath = _path;

	_nsStack.push_back(NameSpace::root());
}


Parser::~Parser()
{
}


inline bool Parser::isIdentifier(const Token* pToken)
{
	return pToken->is(Token::IDENTIFIER_TOKEN) || isOperator(pToken, OperatorToken::OP_DBL_COLON);
}


inline bool Parser::isOperator(const Token* pToken, int kind)
{
	return pToken->is(Token::OPERATOR_TOKEN) && pToken->asInteger() == kind;
}


inline bool Parser::isKeyword(const Token* pToken, int kind)
{
	return pToken->is(Token::KEYWORD_TOKEN) && pToken->asInteger() == kind;
}


inline bool Parser::isEOF(const Token* pToken)
{
	return pToken->is(Token::EOF_TOKEN);
}


void Parser::expectOperator(const Token* pToken, int kind, const std::string& msg)
{
	if (!isOperator(pToken, kind))
		syntaxError(msg + ", found " + pToken->tokenString());
}


void Parser::syntaxError(const std::string& msg)
{
	throw SyntaxException("Expected", msg);
}


inline void Parser::append(std::string& decl, const std::string& token)
{
	if (!decl.empty())
	{
		char last = decl[decl.length() - 1];
		if (token != "::" &&
			token != "." &&
			token != ")" &&
			token != "->" &&
			token != "," &&
			token != "[" &&
			token != "]" &&
			last != '~' &&
			last != ':' &&
			last != '(' &&
			last != ')' &&
			last != '[' &&
			last != ']' &&
			last != ' '
			)
			decl.append(" ");
	}
	decl.append(token);
	if (token == "const"
	 || token == "constexpr"
	 || token == "static"
	 || token == "mutable"
	 || token == "inline"
	 || token == "volatile"
	 || token == "register"
	 || token == "thread_local")
		decl.append(" ");
}


inline void Parser::append(std::string& decl, const Token* pToken)
{
	poco_check_ptr (pToken);
	append(decl, pToken->tokenString());
}


void Parser::parse()
{
	try
	{
		const Token* pNext = next();
		pNext = parseFile(pNext);
		if (!isEOF(pNext))
			syntaxError("Additional tokens behind supposed EOF");
	}
	catch (SyntaxException& exc)
	{
		std::string m(exc.message());
		std::string where(_currentPath);
		where.append("(");
		where.append(NumberFormatter::format(_istr.getCurrentLineNumber()));
		where.append(")");
		throw SyntaxException(m, where);
	}
}


const Token* Parser::parseFile(const Token* pNext)
{
	while (pNext->is(Token::IDENTIFIER_TOKEN) || pNext->is(Token::KEYWORD_TOKEN))
	{
		switch (pNext->asInteger())
		{
		case IdentifierToken::KW_NAMESPACE:
			pNext = parseNameSpace(pNext);
			break;
		case IdentifierToken::KW_STRUCT:
		case IdentifierToken::KW_CLASS:
		case IdentifierToken::KW_UNION:
			pNext = parseClass(pNext);
			break;
		case IdentifierToken::KW_TEMPLATE:
			pNext = parseTemplate(pNext);
			break;
		case IdentifierToken::KW_TYPEDEF:
			pNext = parseTypeDef(pNext);
			break;
		case IdentifierToken::KW_USING:
			pNext = parseUsing(pNext);
			break;
		case IdentifierToken::KW_ENUM:
			pNext = parseEnum(pNext);
			break;
		default:
			pNext = parseVarFunc(pNext);
		}
	}
	return pNext;
}


const Token* Parser::parseNameSpace(const Token* pNext)
{
	poco_assert (isKeyword(pNext, IdentifierToken::KW_NAMESPACE));

	pNext = next();
	if (pNext->is(Token::IDENTIFIER_TOKEN))
	{
		_access = Symbol::ACC_PUBLIC;
		std::string name = pNext->tokenString();
		pNext = next();
		expectOperator(pNext, OperatorToken::OP_OPENBRACE, "{");

		std::string fullName = currentNameSpace()->fullName();
		if (!fullName.empty()) fullName += "::";
		fullName += name;

		NameSpace* pNS = dynamic_cast<NameSpace*>(currentNameSpace()->lookup(fullName));
		bool undefined = (pNS == 0);
		if (undefined) pNS = new NameSpace(name, currentNameSpace());
		pushNameSpace(pNS, -1, undefined);
		pNext = next();
		while (pNext->is(Token::IDENTIFIER_TOKEN) || pNext->is(Token::KEYWORD_TOKEN))
		{
			switch (pNext->asInteger())
			{
			case IdentifierToken::KW_NAMESPACE:
				pNext = parseNameSpace(pNext);
				break;
			case IdentifierToken::KW_STRUCT:
			case IdentifierToken::KW_CLASS:
			case IdentifierToken::KW_UNION:
				pNext = parseClass(pNext);
				break;
			case IdentifierToken::KW_TEMPLATE:
				pNext = parseTemplate(pNext);
				break;
			case IdentifierToken::KW_TYPEDEF:
				pNext = parseTypeDef(pNext);
				break;
			case IdentifierToken::KW_USING:
				pNext = parseUsing(pNext);
				break;
			case IdentifierToken::KW_ENUM:
				pNext = parseEnum(pNext);
				break;
			default:
				pNext = parseVarFunc(pNext);
			}
		}
		expectOperator(pNext, OperatorToken::OP_CLOSBRACE, "}");
		pNext = next();
	}
	else syntaxError("namespace name");
	popNameSpace();
	return pNext;
}


const Token* Parser::parseClass(const Token* pNext)
{
	std::string decl;
	return parseClass(pNext, decl);
}


const Token* Parser::parseClass(const Token* pNext, std::string& decl)
{
	poco_assert (isKeyword(pNext, IdentifierToken::KW_CLASS) || isKeyword(pNext, IdentifierToken::KW_STRUCT) || isKeyword(pNext, IdentifierToken::KW_UNION));

	_pCurrentSymbol = 0;
	bool isClass = isKeyword(pNext, IdentifierToken::KW_CLASS);
	int line = _istr.getCurrentLineNumber();
	Symbol::Access prevAccess = _access;
	append(decl, pNext);
	Symbol::Access access;
	if (isKeyword(pNext, IdentifierToken::KW_CLASS))
		access = Symbol::ACC_PRIVATE;
	else
		access = Symbol::ACC_PUBLIC;
	pNext = next();
	if (pNext->is(Token::IDENTIFIER_TOKEN))
		append(decl, pNext);
	else
		syntaxError("class/struct name");
	pNext = next();
	bool isFinal = false;
	if (isIdentifier(pNext) && pNext->asString() == "final")
	{
		pNext = next();
		isFinal = true;
	}
	if (!isOperator(pNext, OperatorToken::OP_SEMICOLON))
	{
		// if we have a template specialization the next token will be a <
		if (isOperator(pNext, OperatorToken::OP_LT))
		{
			// skip all template specializations
			// skip until at { bracket, then parseBlock to ignore it
			while (!isOperator(pNext, OperatorToken::OP_OPENBRACE))
				pNext = next();
			pNext = parseBlock(pNext); // skip after }

			expectOperator(pNext, OperatorToken::OP_SEMICOLON, ";");
			pNext = next();
			_access = prevAccess;
			_pCurrentSymbol = 0;
			return pNext;
		}
		if (isOperator(pNext, OperatorToken::OP_COLON) || isOperator(pNext, OperatorToken::OP_OPENBRACE))
		{
			Struct* pClass = new Struct(decl, isClass, currentNameSpace());
			if (isFinal) pClass->makeFinal();
			pushNameSpace(pClass, line);
			_access = access;
			if (isOperator(pNext, OperatorToken::OP_COLON))
				pNext = parseBaseClassList(next(), pClass);
			expectOperator(pNext, OperatorToken::OP_OPENBRACE, "{");
			pNext = parseClassMembers(pNext, pClass);
			expectOperator(pNext, OperatorToken::OP_CLOSBRACE, "}");
			pNext = next();
			expectOperator(pNext, OperatorToken::OP_SEMICOLON, ";");
			popNameSpace();
		}
		else return parseVarFunc(pNext, decl);
	}
	pNext = next();
	_access = prevAccess;
	_pCurrentSymbol = 0;
	return pNext;
}


const Token* Parser::parseBaseClassList(const Token* pNext, Struct* pClass)
{
	while (pNext->is(Token::IDENTIFIER_TOKEN) || pNext->is(Token::KEYWORD_TOKEN))
	{
		bool isVirtual = false;
		Symbol::Access acc = _access;
		while (pNext->is(Token::KEYWORD_TOKEN))
		{
			switch (pNext->asInteger())
			{
			case IdentifierToken::KW_PUBLIC:
				acc = Symbol::ACC_PUBLIC;
				break;
			case IdentifierToken::KW_PROTECTED:
				acc = Symbol::ACC_PROTECTED;
				break;
			case IdentifierToken::KW_PRIVATE:
				acc = Symbol::ACC_PRIVATE;
				break;
			case IdentifierToken::KW_VIRTUAL:
				isVirtual = true;
				break;
			default:
				syntaxError("public, protected, private or virtual");
			}
			pNext = next();
		}
		std::string id;
		pNext = parseIdentifier(pNext, id);
		if (isOperator(pNext, OperatorToken::OP_LT))
			pNext = parseTemplateArgs(pNext, id);
		pClass->addBase(id, acc, isVirtual);
		if (isOperator(pNext, OperatorToken::OP_COMMA))
			pNext = next();
	}
	return pNext;
}


const Token* Parser::parseClassMembers(const Token* pNext, Struct* pClass)
{
	poco_assert (isOperator(pNext, OperatorToken::OP_OPENBRACE));

	pNext = next();
	while (pNext->is(Token::IDENTIFIER_TOKEN) || pNext->is(Token::KEYWORD_TOKEN) || isOperator(pNext, OperatorToken::OP_COMPL))
	{
		switch (pNext->asInteger())
		{
		case IdentifierToken::KW_PRIVATE:
		case IdentifierToken::KW_PROTECTED:
		case IdentifierToken::KW_PUBLIC:
			pNext = parseAccess(pNext);
			break;
		case IdentifierToken::KW_STRUCT:
		case IdentifierToken::KW_CLASS:
		case IdentifierToken::KW_UNION:
			pNext = parseClass(pNext);
			break;
		case IdentifierToken::KW_TEMPLATE:
			pNext = parseTemplate(pNext);
			break;
		case IdentifierToken::KW_TYPEDEF:
			pNext = parseTypeDef(pNext);
			break;
		case IdentifierToken::KW_ENUM:
			pNext = parseEnum(pNext);
			break;
		case IdentifierToken::KW_FRIEND:
			pNext = parseFriend(pNext);
			break;
		case OperatorToken::OP_COMPL:
		default:
			pNext = parseVarFunc(pNext);
		}
	}
	return pNext;
}


const Token* Parser::parseAccess(const Token* pNext)
{
	switch (pNext->asInteger())
	{
	case IdentifierToken::KW_PRIVATE:
		_access = Symbol::ACC_PRIVATE;
		break;
	case IdentifierToken::KW_PROTECTED:
		_access = Symbol::ACC_PROTECTED;
		break;
	case IdentifierToken::KW_PUBLIC:
		_access = Symbol::ACC_PUBLIC;
		break;
	}
	pNext = next();
	expectOperator(pNext, OperatorToken::OP_COLON, ":");
	return next();
}


const Token* Parser::parseTemplate(const Token* pNext)
{
	poco_assert (isKeyword(pNext, IdentifierToken::KW_TEMPLATE));

	std::string decl;
	append(decl, pNext);
	pNext = next();
	expectOperator(pNext, OperatorToken::OP_LT, "<");
	pNext = parseTemplateArgs(pNext, decl);
	if (isKeyword(pNext, IdentifierToken::KW_CLASS) || isKeyword(pNext, IdentifierToken::KW_STRUCT) || isKeyword(pNext, IdentifierToken::KW_UNION))
		return parseClass(pNext, decl);
	else
		return parseVarFunc(pNext, decl);
}


const Token* Parser::parseTemplateArgs(const Token* pNext, std::string& decl)
{
	poco_assert (isOperator(pNext, OperatorToken::OP_LT));

	append(decl, pNext);
	int depth = 1;
	pNext = next();
	while (depth > 0 && !isEOF(pNext))
	{
		append(decl, pNext);
		if (isOperator(pNext, OperatorToken::OP_LT))
			++depth;
		else if (isOperator(pNext, OperatorToken::OP_GT))
			--depth;
		pNext = next();
	}
	return pNext;
}


const Token* Parser::parseTypeDef(const Token* pNext)
{
	poco_assert (isKeyword(pNext, IdentifierToken::KW_TYPEDEF));

	_pCurrentSymbol = 0;
	int line = _istr.getCurrentLineNumber();
	std::string decl;
	while (!isOperator(pNext, OperatorToken::OP_SEMICOLON) && !isEOF(pNext))
	{
		append(decl, pNext);
		pNext = next();
	}
	TypeDef* pTypeDef = new TypeDef(decl, currentNameSpace());
	addSymbol(pTypeDef, line);
	pNext = next();
	_pCurrentSymbol = 0;
	return pNext;
}


const Token* Parser::parseUsing(const Token* pNext)
{
	poco_assert (isKeyword(pNext, IdentifierToken::KW_USING));

	_pCurrentSymbol = 0;
	int line = _istr.getCurrentLineNumber();
	pNext = next();
	if (isKeyword(pNext, IdentifierToken::KW_NAMESPACE))
	{
		pNext = next();
		if (isIdentifier(pNext))
		{
			std::string ns;
			pNext = parseIdentifier(pNext, ns);
			currentNameSpace()->importNameSpace(ns);
		}
		else syntaxError("identifier");
	}
	else
	{
		if (isIdentifier(pNext))
		{
			std::string id;
			pNext = parseIdentifier(pNext, id);
			if (isOperator(pNext, OperatorToken::OP_ASSIGN))
			{
				pNext = next();
				std::string decl("using ");
				decl += id;
				decl += " = ";
				while (!isOperator(pNext, OperatorToken::OP_SEMICOLON) && !isEOF(pNext))
				{
					append(decl, pNext);
					pNext = next();
				}
				TypeAlias* pTypeAlias = new TypeAlias(decl, currentNameSpace());
				addSymbol(pTypeAlias, line);
			}
			else
			{
				currentNameSpace()->importSymbol(id);
			}
		}
	}

	if (!isOperator(pNext, OperatorToken::OP_SEMICOLON))
		syntaxError("semicolon");
	pNext = next();
	_pCurrentSymbol = 0;
	return pNext;
}


const Token* Parser::parseFriend(const Token* pNext)
{
	poco_assert (isKeyword(pNext, IdentifierToken::KW_FRIEND));

	pNext = next();

	while (!isOperator(pNext, OperatorToken::OP_SEMICOLON) && !isEOF(pNext))
		pNext = next();

	if (isOperator(pNext, OperatorToken::OP_SEMICOLON))
		pNext = next();
	return pNext;
}


const Token* Parser::parseVarFunc(const Token* pNext)
{
	std::string decl;
	return parseVarFunc(pNext, decl);
}


const Token* Parser::parseVarFunc(const Token* pNext, std::string& decl)
{
	_pCurrentSymbol = 0;
	if (isKeyword(pNext, IdentifierToken::KW_EXTERN))
	{
		pNext = parseExtern(pNext);
	}
	else
	{
		append(decl, pNext);
		pNext = next();
		bool isOp = false;
		while (!isOperator(pNext, OperatorToken::OP_SEMICOLON) && !isOperator(pNext, OperatorToken::OP_OPENPARENT) && !isEOF(pNext))
		{
			append(decl, pNext);
			isOp = isKeyword(pNext, IdentifierToken::KW_OPERATOR);
			pNext = next();
		}
		if (isOperator(pNext, OperatorToken::OP_SEMICOLON))
		{
			std::string name = Symbol::extractName(decl);
			if (!currentNameSpace()->lookup(name))
			{
				Variable* pVar = new Variable(decl, currentNameSpace());
				addSymbol(pVar, _istr.getCurrentLineNumber());
			}
			pNext = next();
		}
		else if (isOperator(pNext, OperatorToken::OP_OPENPARENT))
		{
			if (isOp)
			{
				decl += " (";
				pNext = next();
				expectOperator(pNext, OperatorToken::OP_CLOSPARENT, ")");
				append(decl, pNext);
				pNext = next();
				expectOperator(pNext, OperatorToken::OP_OPENPARENT, "(");
			}
			pNext = parseFunc(pNext, decl);
		}
	}
	_pCurrentSymbol = 0;
	return pNext;
}


const Token* Parser::parseExtern(const Token* pNext)
{
	poco_assert (isKeyword(pNext, IdentifierToken::KW_EXTERN));

	pNext = next();
	if (pNext->is(Token::STRING_LITERAL_TOKEN))
		pNext = next();
	if (isOperator(pNext, OperatorToken::OP_OPENBRACE))
	{
		pNext = parseBlock(pNext);
	}
	else
	{
		while (!isOperator(pNext, OperatorToken::OP_SEMICOLON) && !isEOF(pNext))
			pNext = next();
	}
	if (isOperator(pNext, OperatorToken::OP_SEMICOLON))
		pNext = next();
	return pNext;
}


const Token* Parser::parseFunc(const Token* pNext, std::string& decl)
{
	poco_assert (isOperator(pNext, OperatorToken::OP_OPENPARENT));

	int line = _istr.getCurrentLineNumber();
	Function* pFunc = 0;
	std::string name = Symbol::extractName(decl);
	if (name.find(':') == std::string::npos)
	{
		pFunc = new Function(decl, currentNameSpace());
		addSymbol(pFunc, line);
	}
	pNext = parseParameters(pNext, pFunc);
	expectOperator(pNext, OperatorToken::OP_CLOSPARENT, ")");
	pNext = next();
	while (pNext->is(Poco::Token::IDENTIFIER_TOKEN) || pNext->is(Poco::Token::KEYWORD_TOKEN))
	{
		if (isKeyword(pNext, IdentifierToken::KW_CONST))
		{
			if (pFunc) pFunc->makeConst();
			pNext = next();
		}
		if (isKeyword(pNext, IdentifierToken::KW_THROW))
		{
			while (!isOperator(pNext, OperatorToken::OP_ASSIGN) && !isOperator(pNext, OperatorToken::OP_SEMICOLON) &&
				   !isOperator(pNext, OperatorToken::OP_OPENBRACE) && !isEOF(pNext))
				pNext = next();
		}
		else if (isKeyword(pNext, IdentifierToken::KW_NOEXCEPT))
		{
			if (pFunc) pFunc->makeNoexcept();
			pNext = next();
		}
		else if (isIdentifier(pNext) && pNext->asString() == "override")
		{
			if (pFunc) pFunc->makeOverride();
			pNext = next();
		}
		else if (isIdentifier(pNext) && pNext->asString() == "final")
		{
			if (pFunc) pFunc->makeFinal();
			pNext = next();
		}
		else if (isKeyword(pNext, IdentifierToken::KW_TRY))
		{
			break; // handled below
		}
	}
	if (isOperator(pNext, OperatorToken::OP_ASSIGN))
	{
		pNext = next();
		if (!pNext->is(Token::INTEGER_LITERAL_TOKEN) && !isKeyword(pNext, IdentifierToken::KW_DEFAULT) && !isKeyword(pNext, IdentifierToken::KW_DELETE))
			syntaxError("0, default or delete");
		if (isKeyword(pNext, IdentifierToken::KW_DEFAULT))
			pFunc->makeDefault();
		else if (isKeyword(pNext, IdentifierToken::KW_DELETE))
			pFunc->makeDelete();
		pNext = next();
		if (pFunc) pFunc->makePureVirtual();
		expectOperator(pNext, OperatorToken::OP_SEMICOLON, ";");
	}
	else if (isOperator(pNext, OperatorToken::OP_OPENBRACE) || isOperator(pNext, OperatorToken::OP_COLON))
	{
		while (!isOperator(pNext, OperatorToken::OP_OPENBRACE) && !isEOF(pNext))
			pNext = next();

		pNext = parseBlock(pNext);
		if (!pFunc)
			pFunc = dynamic_cast<Function*>(currentNameSpace()->lookup(name));
		if (pFunc)
			pFunc->makeInline();
	}
	else if (isKeyword(pNext, IdentifierToken::KW_TRY))
	{
		pNext = next();
		if (isOperator(pNext, OperatorToken::OP_OPENBRACE) || isOperator(pNext, OperatorToken::OP_COLON))
		{
			while (!isOperator(pNext, OperatorToken::OP_OPENBRACE) && !isEOF(pNext))
				pNext = next();

			pNext = parseBlock(pNext);

			if (isKeyword(pNext, IdentifierToken::KW_CATCH))
			{
				while (!isOperator(pNext, OperatorToken::OP_OPENBRACE) && !isEOF(pNext))
					pNext = next();

				pNext = parseBlock(pNext);
			}
			else syntaxError("expected catch block");

			if (!pFunc)
				pFunc = dynamic_cast<Function*>(currentNameSpace()->lookup(name));
			if (pFunc)
				pFunc->makeInline();
		}
		else syntaxError("expected member initialization or block");
	}
	else
	{
		expectOperator(pNext, OperatorToken::OP_SEMICOLON, ";");
	}
	if (isOperator(pNext, OperatorToken::OP_SEMICOLON))
		pNext = next();
	return pNext;
}


const Token* Parser::parseParameters(const Token* pNext, Function* pFunc)
{
	poco_assert (isOperator(pNext, OperatorToken::OP_OPENPARENT));

	pNext = next();
	while (!isOperator(pNext, OperatorToken::OP_CLOSPARENT) && !isEOF(pNext))
	{
		std::string decl;
		int depth = 0;
		int tdepth = 0;
		while ((depth > 0 || tdepth > 0 || (!isOperator(pNext, OperatorToken::OP_CLOSPARENT) && !isOperator(pNext, OperatorToken::OP_COMMA))) && !isEOF(pNext))
		{
			append(decl, pNext);
			if (isOperator(pNext, OperatorToken::OP_OPENPARENT))
				++depth;
			else if (isOperator(pNext, OperatorToken::OP_CLOSPARENT))
				--depth;
			else if (isOperator(pNext, OperatorToken::OP_LT))
				++tdepth;
			else if (isOperator(pNext, OperatorToken::OP_GT))
				--tdepth;
			pNext = next();
		}
		if (isOperator(pNext, OperatorToken::OP_COMMA))
			pNext = next();
		if (pFunc && decl != "void") // don't add the void parameter, I simply check here to avoid throwing away void*
			pFunc->addParameter(new Parameter(decl, pFunc));
	}
	return pNext;
}


const Token* Parser::parseBlock(const Token* pNext)
{
	poco_assert (isOperator(pNext, OperatorToken::OP_OPENBRACE));

	pNext = next();
	int depth = 1;
	while (depth > 0 && !isEOF(pNext))
	{
		if (isOperator(pNext, OperatorToken::OP_OPENBRACE))
			++depth;
		else if (isOperator(pNext, OperatorToken::OP_CLOSBRACE))
			--depth;
		pNext = next();
	}
	return pNext;
}


const Token* Parser::parseEnum(const Token* pNext)
{
	poco_assert (isKeyword(pNext, IdentifierToken::KW_ENUM));

	_pCurrentSymbol = 0;
	int line = _istr.getCurrentLineNumber();
	pNext = next();
	std::string name;
	if (pNext->is(Token::IDENTIFIER_TOKEN))
	{
		name = pNext->tokenString();
		pNext = next();
	}
	expectOperator(pNext, OperatorToken::OP_OPENBRACE, "{");
	Enum* pEnum = new Enum(name, currentNameSpace());
	addSymbol(pEnum, line);
	pNext = next();
	while (pNext->is(Token::IDENTIFIER_TOKEN))
	{
		pNext = parseEnumValue(pNext, pEnum);
	}
	expectOperator(pNext, OperatorToken::OP_CLOSBRACE, "}");
	pNext = next();
	expectOperator(pNext, OperatorToken::OP_SEMICOLON, ";");
	pNext = next();
	_pCurrentSymbol = 0;
	return pNext;
}


const Token* Parser::parseEnumValue(const Token* pNext, Enum* pEnum)
{
	_pCurrentSymbol = 0;
	_doc.clear();
	int line = _istr.getCurrentLineNumber();
	std::string name = pNext->tokenString();
	std::string value;
	pNext = next();
	if (isOperator(pNext, OperatorToken::OP_ASSIGN))
	{
		pNext = next();
		while (!isOperator(pNext, OperatorToken::OP_COMMA) && !isOperator(pNext, OperatorToken::OP_CLOSBRACE) && !isEOF(pNext))
		{
			append(value, pNext);
			pNext = next();
		}
	}
	EnumValue* pValue = new EnumValue(name, value, pEnum);
	addSymbol(pValue, line, true);
	if (isOperator(pNext, OperatorToken::OP_COMMA))
		pNext = next();
	else if (!isOperator(pNext, OperatorToken::OP_CLOSBRACE))
		syntaxError(", or }");
	return pNext;
}


const Token* Parser::parseIdentifier(const Token* pNext, std::string& id)
{
	poco_assert (pNext->is(Token::IDENTIFIER_TOKEN) || isOperator(pNext, OperatorToken::OP_DBL_COLON));
	id.append(pNext->tokenString());
	if (isOperator(pNext, OperatorToken::OP_DBL_COLON))
	{
		pNext = next();
		if (!pNext->is(Token::IDENTIFIER_TOKEN)) syntaxError("identifier");
		id.append(pNext->tokenString());
	}
	pNext = next();
	while (isOperator(pNext, OperatorToken::OP_DBL_COLON))
	{
		id.append("::");
		pNext = next();
		if (!pNext->is(Token::IDENTIFIER_TOKEN)) syntaxError("identifier");
		id.append(pNext->tokenString());
		pNext = next();
	}
	return pNext;
}


void Parser::addSymbol(Symbol* pSymbol, int lineNumber, bool addGST)
{
	pSymbol->setLineNumber(lineNumber);
	pSymbol->setFile(_currentPath);
	pSymbol->setPackage(_package);
	pSymbol->setLibrary(_library);
	pSymbol->setAccess(_access);
	_pCurrentSymbol = pSymbol;
	if (addGST)
		_gst.insert(NameSpace::SymbolTable::value_type(pSymbol->name(), pSymbol));
	if (!_doc.empty())
		pSymbol->addDocumentation(_doc);
	if (!_attrs.empty())
	{
		Attributes attrs;
		std::istringstream istr(_attrs);
		AttributesParser parser(attrs, istr);
		parser.parse();
		pSymbol->setAttributes(attrs);
		_attrs.clear();
	}
	_doc.clear();
}


void Parser::pushNameSpace(NameSpace* pNameSpace, int lineNumber, bool addGST)
{
	addSymbol(pNameSpace, lineNumber, addGST);
	_nsStack.push_back(pNameSpace);
}


void Parser::popNameSpace()
{
	_nsStack.pop_back();
}


NameSpace* Parser::currentNameSpace() const
{
	return _nsStack.back();
}


const Token* Parser::next()
{
	const Token* pNext = nextPreprocessed();
	while (!_inFile && !isEOF(pNext))
		pNext = nextPreprocessed();
	return pNext;
}


const Token* Parser::nextPreprocessed()
{
	const Token* pNext = nextToken();
	while (pNext->is(Token::PREPROCESSOR_TOKEN))
	{
		std::istringstream pps(pNext->tokenString());
		pps.get();
		Tokenizer ppt(pps);
		const Token* pPPT = ppt.next();
		if (pPPT->tokenString() == "line" || pPPT->is(Token::INTEGER_LITERAL_TOKEN))
		{
			if (!pPPT->is(Token::INTEGER_LITERAL_TOKEN))
				pPPT = ppt.next();
			int line = pPPT->asInteger();
			_istr.setCurrentLineNumber(line);
			pPPT = ppt.next();
			if (pPPT->is(Token::STRING_LITERAL_TOKEN))
			{
				std::string path = pPPT->asString();
				Path p(path);
				p.makeAbsolute();
				_currentPath = p.toString();
				_inFile = (icompare(_path, _currentPath) == 0);
			}
		}
		pNext = nextToken();
	}
	return pNext;
}


const Token* Parser::nextToken()
{
	const Token* pNext = _tokenizer.next();
	while (pNext->is(Token::COMMENT_TOKEN) || pNext->is(Token::SPECIAL_COMMENT_TOKEN))
	{
		if (pNext->is(Token::SPECIAL_COMMENT_TOKEN))
		{
			if (_pCurrentSymbol)
			{
				_pCurrentSymbol->addDocumentation(pNext->asString());
				_doc.clear();
			}
			else if (_inFile)
			{
				if (!_doc.empty()) _doc += "\n";
				_doc += pNext->asString();
			}
		}
		else if (pNext->is(Token::COMMENT_TOKEN) && _inFile)
		{
			const std::string& comment = pNext->tokenString();
			if (comment.compare(0, 3, "//@") == 0)
			{
				_attrs.append(comment.substr(3));
			}
			else if (comment.compare(0, 11, "// Package:") == 0)
			{
				_package = comment.substr(11);
				trimInPlace(_package);
			}
			else if (comment.compare(0, 11, "// Library:") == 0)
			{
				_library = comment.substr(11);
				trimInPlace(_library);
			}
		}
		pNext = _tokenizer.next();
	}
	return pNext;
}


} } // namespace Poco::CppParser
