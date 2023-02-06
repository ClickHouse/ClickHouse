//
// CppToken.h
//
// Library: CppParser
// Package: CppParser
// Module:  CppToken
//
// Definition of the CppToken class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CppParser_CppToken_INCLUDED
#define CppParser_CppToken_INCLUDED


#include "Poco/CppParser/CppParser.h"
#include "Poco/Token.h"
#include <map>


namespace Poco {
namespace CppParser {


class CppParser_API CppToken: public Poco::Token
	/// The base class for all C++ tokens.
{
public:
	CppToken();
	~CppToken();
	
protected:
	void syntaxError(const std::string& expected, const std::string& actual);
};


class CppParser_API OperatorToken: public CppToken
{
public:
	enum Tokens
	{
		OP_OPENBRACKET = 1, // [
		OP_CLOSBRACKET,     // ]
		OP_OPENPARENT,      // (
		OP_CLOSPARENT,      // )
		OP_OPENBRACE,       // {
		OP_CLOSBRACE,       // }
		OP_LT,              // <
		OP_LE,              // <=
		OP_SHL,             // <<
		OP_SHL_ASSIGN,		// <<=
		OP_GT,              // >
		OP_GE,              // >=
		OP_SHR,             // >>
		OP_SHR_ASSIGN,      // >>=
		OP_ASSIGN,          // =
		OP_EQ,              // ==
		OP_NOT,             // !
		OP_NE,              // !=
		OP_BITAND,          // &
		OP_BITAND_ASSIGN,   // &=
		OP_AND,             // &&
		OP_BITOR,           // |
		OP_BITOR_ASSIGN,    // |= 
		OP_OR,              // ||
		OP_XOR,             // ^
		OP_XOR_ASSIGN,      // ^=
		OP_COMPL,           // ~
		OP_ASTERISK,        // *
		OP_ASTERISK_ASSIGN, // *=
		OP_SLASH,           // /
		OP_SLASH_ASSIGN,    // /=
		OP_PLUS,            // +
		OP_PLUS_ASSIGN,     // +=
		OP_INCR,            // ++
		OP_MINUS,           // -
		OP_MINUS_ASSIGN,    // -=
		OP_DECR,            // --
		OP_ARROW,           // ->
		OP_MOD,             // %
		OP_MOD_ASSIGN,      // %=
		OP_COMMA,           // ,
		OP_PERIOD,          // .
		OP_TRIPLE_PERIOD,   // ...
		OP_COLON,           // :
		OP_DBL_COLON,       // ::
		OP_SEMICOLON,       // ;
		OP_QUESTION         // ?
	};
	
	OperatorToken();
	~OperatorToken();
	Poco::Token::Class tokenClass() const;
	bool start(char c, std::istream& istr);
	void finish(std::istream& istr);
	int asInteger() const;
	
private:
	typedef std::map<std::string, int> OpMap;
	
	OpMap _opMap;
};


class CppParser_API IdentifierToken: public CppToken
{
public:
	enum Keywords
	{
		KW_ALIGNAS = 1,
		KW_ALIGNOF,
		KW_AND,
		KW_AND_EQ,
		KW_ASM,
		KW_AUTO,
		KW_BITAND,
		KW_BITOR,
		KW_BOOL,
		KW_BREAK,
		KW_CASE,
		KW_CATCH,
		KW_CHAR,
		KW_CHAR_16T,
		KW_CHAR_32T,
		KW_CLASS,
		KW_COMPL,
		KW_CONST,
		KW_CONSTEXPR,
		KW_CONST_CAST,
		KW_CONTINUE,
		KW_DECLTYPE,
		KW_DEFAULT,
		KW_DELETE,
		KW_DO,
		KW_DOUBLE,
		KW_DYNAMIC_CAST,
		KW_ELSE,
		KW_ENUM,
		KW_EXPLICIT,
		KW_EXPORT,
		KW_EXTERN,
		KW_FALSE,
		KW_FLOAT,
		KW_FOR,
		KW_FRIEND,
		KW_GOTO,
		KW_IF,
		KW_INLINE,
		KW_INT,
		KW_LONG,
		KW_MUTABLE,
		KW_NAMESPACE,
		KW_NEW,
		KW_NOEXCEPT,
		KW_NOT,
		KW_NOT_EQ,
		KW_NULLPTR,
		KW_OPERATOR,
		KW_OR,
		KW_OR_EQ,
		KW_PRIVATE,
		KW_PROTECTED,
		KW_PUBLIC,
		KW_REGISTER,
		KW_REINTERPRET_CAST,
		KW_RETURN,
		KW_SHORT,
		KW_SIGNED,
		KW_SIZEOF,
		KW_STATIC,
		KW_STATIC_ASSERT,
		KW_STATIC_CAST,
		KW_STRUCT,
		KW_SWITCH,
		KW_TEMPLATE,
		KW_THIS,
		KW_THREAD_LOCAL,
		KW_THROW,
		KW_TRUE,
		KW_TRY,
		KW_TYPEDEF,
		KW_TYPEID,
		KW_TYPENAME,
		KW_UNION,
		KW_UNSIGNED,
		KW_USING,
		KW_VIRTUAL,
		KW_VOID,
		KW_VOLATILE,
		KW_WCHAR_T,
		KW_WHILE,
		KW_XOR,
		KW_XOR_EQ
	};
	
	IdentifierToken();
	~IdentifierToken();
	Poco::Token::Class tokenClass() const;
	bool start(char c, std::istream& istr);
	void finish(std::istream& istr);
	int asInteger() const;
	
private:
	typedef std::map<std::string, int> KWMap;
	
	KWMap _kwMap;
};


class CppParser_API StringLiteralToken: public CppToken
{
public:
	StringLiteralToken();
	~StringLiteralToken();
	Poco::Token::Class tokenClass() const;
	bool start(char c, std::istream& istr);
	void finish(std::istream& istr);
	std::string asString() const;
};


class CppParser_API CharLiteralToken: public CppToken
{
public:
	CharLiteralToken();
	~CharLiteralToken();
	Poco::Token::Class tokenClass() const;
	bool start(char c, std::istream& istr);
	void finish(std::istream& istr);
	char asChar() const;
};


class CppParser_API NumberLiteralToken: public CppToken
{
public:
	NumberLiteralToken();
	~NumberLiteralToken();
	Poco::Token::Class tokenClass() const;
	bool start(char c, std::istream& istr);
	void finish(std::istream& istr);
	int asInteger() const;
	double asFloat() const;
	
private:
	bool _isFloat;
};


class CppParser_API CommentToken: public CppToken
{
public:
	CommentToken();
	~CommentToken();
	Poco::Token::Class tokenClass() const;
	bool start(char c, std::istream& istr);
	void finish(std::istream& istr);
	std::string asString() const;
};


class CppParser_API PreprocessorToken: public CppToken
{
public:
	PreprocessorToken();
	~PreprocessorToken();
	Poco::Token::Class tokenClass() const;
	bool start(char c, std::istream& istr);
	void finish(std::istream& istr);
};


} } // namespace Poco::CppParser


#endif // CppParser_CppToken_INCLUDED
