//
// CppToken.cpp
//
// Library: CppParser
// Package: CppParser
// Module:  CppToken
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/CppParser/CppToken.h"
#include "Poco/Exception.h"
#include "Poco/NumberParser.h"
#include <cctype>
#include <cstdlib>


using Poco::Token;
using Poco::SyntaxException;


namespace Poco {
namespace CppParser {


CppToken::CppToken()
{
}


CppToken::~CppToken()
{
}


void CppToken::syntaxError(const std::string& expected, const std::string& actual)
{
	std::string msg("expected: ");
	msg.append(expected);
	msg.append(", got: ");
	msg.append(actual);
	throw SyntaxException(msg);
}


OperatorToken::OperatorToken()
{
	int i = 1;
	_opMap["["] = i++;
	_opMap["]"] = i++;
	_opMap["("] = i++;
	_opMap[")"] = i++;
	_opMap["{"] = i++;
	_opMap["}"] = i++;
	_opMap["<"] = i++;
	_opMap["<="] = i++;
	_opMap["<<"] = i++;
	_opMap["<<="] = i++;
	_opMap[">"] = i++;
	_opMap[">="] = i++;
	_opMap[">>"] = i++;
	_opMap[">>="] = i++;
	_opMap["="] = i++;
	_opMap["=="] = i++;
	_opMap["!"] = i++;
	_opMap["!="] = i++;
	_opMap["&"] = i++;
	_opMap["&="] = i++;
	_opMap["&&"] = i++;
	_opMap["|"] = i++;
	_opMap["|="] = i++;
	_opMap["||"] = i++;
	_opMap["^"] = i++;
	_opMap["^="] = i++;
	_opMap["~"] = i++;
	_opMap["*"] = i++;
	_opMap["*="] = i++;
	_opMap["/"] = i++;
	_opMap["/="] = i++;
	_opMap["+"] = i++;
	_opMap["+="] = i++;
	_opMap["++"] = i++;
	_opMap["-"] = i++;
	_opMap["-="] = i++;
	_opMap["--"] = i++;
	_opMap["->"] = i++;
	_opMap["%"] = i++;
	_opMap["%="] = i++;
	_opMap[","] = i++;
	_opMap["."] = i++;
	_opMap["..."] = i++;
	_opMap[":"] = i++;
	_opMap["::"] = i++;
	_opMap[";"] = i++;
	_opMap["?"] = i++;	 
}


OperatorToken::~OperatorToken()
{
}


Token::Class OperatorToken::tokenClass() const
{
	return Token::OPERATOR_TOKEN;
}


bool OperatorToken::start(char c, std::istream& istr)
{
	_value = c;
	char next = (char) istr.peek();
	switch (_value[0])
	{
	case '[':
	case ']':
	case '(':
	case ')':
	case '{':
	case '}':
	case '<':
	case '>':
	case '=':
	case '!':
	case '&':
	case '|':
	case '*':
	case '+':
	case '-':
	case '^':
	case '~':
	case ',':
	case ':':
	case ';':
	case '%':
	case '?':
		return true;
	case '.':
		return !(next >= '0' && next <= '9');
	case '/':
		return !(next == '/' || next == '*');
	default:
		return false;
	}
}


void OperatorToken::finish(std::istream& istr)
{
	int next = (char) istr.peek();
	switch (_value[0])
	{
	case '(':
	case ')':
	case '{':
	case '}':
	case '[':
	case ']':
	case ';':
	case '?':
	case '~':
	case ',':
		break;
	case '.':
		if (next == '.')
		{
			_value += (char) istr.get();
			if (istr.peek() != '.') syntaxError(".", std::string(1, (char) istr.peek()));
			_value += (char) istr.get();
		}
		break;
	case ':':
		if (next == ':') _value += (char) istr.get();
		break;
	case '<':
		if (next == '<')
		{
			_value += (char) istr.get();
			next = (char) istr.peek();
		}
		if (next == '=') _value += (char) istr.get();
		break;
	case '>':
		if (next == '>')
		{
			_value += (char) istr.get();
			next = (char) istr.peek();
		}
		if (next == '=') _value += (char) istr.get();
		break;
	case '&':
		if (next == '&' || next == '=') _value += (char) istr.get();
		break;
	case '|':
		if (next == '|' || next == '=') _value += (char) istr.get();
		break;
	case '+':
		if (next == '+' || next == '=') _value += (char) istr.get();
		break;
	case '-':
		if (next == '-' || next == '=' || next == '>') _value += (char) istr.get();
		break;
	case '=':
	case '!':
	case '*':
	case '/':
	case '^':
	case '%':
		if (next == '=') _value += (char) istr.get();
		break;
	default:
		poco_bugcheck();
	}
}


int OperatorToken::asInteger() const
{
	OpMap::const_iterator it = _opMap.find(_value);
	if (it != _opMap.end())
		return it->second;
	else
		return 0;
}


IdentifierToken::IdentifierToken()
{
	int i = 1;
	_kwMap["alignas"] = i++;
	_kwMap["alignof"] = i++;
	_kwMap["and"] = i++;
	_kwMap["and_eq"] = i++;
	_kwMap["asm"] = i++;
	_kwMap["auto"] = i++;
	_kwMap["bitand"] = i++;
	_kwMap["bitor"] = i++;
	_kwMap["bool"] = i++;
	_kwMap["break"] = i++;
	_kwMap["case"] = i++;
	_kwMap["catch"] = i++;
	_kwMap["char"] = i++;
	_kwMap["char16_t"] = i++;
	_kwMap["char32_t"] = i++;
	_kwMap["class"] = i++;
	_kwMap["compl"] = i++;
	_kwMap["const"] = i++;
	_kwMap["constexpr"] = i++;
	_kwMap["const_cast"] = i++;
	_kwMap["continue"] = i++;
	_kwMap["decltype"] = i++;
	_kwMap["default"] = i++;
	_kwMap["delete"] = i++;
	_kwMap["do"] = i++;
	_kwMap["double"] = i++;
	_kwMap["dynamic_cast"] = i++;
	_kwMap["else"] = i++;
	_kwMap["enum"] = i++;
	_kwMap["explicit"] = i++;
	_kwMap["export"] = i++;
	_kwMap["extern"] = i++;
	_kwMap["false"] = i++;
	_kwMap["float"] = i++;
	_kwMap["for"] = i++;
	_kwMap["friend"] = i++;
	_kwMap["goto"] = i++;
	_kwMap["if"] = i++;
	_kwMap["inline"] = i++;
	_kwMap["int"] = i++;
	_kwMap["long"] = i++;
	_kwMap["mutable"] = i++;
	_kwMap["namespace"] = i++;
	_kwMap["new"] = i++;
	_kwMap["noexcept"] = i++;
	_kwMap["not"] = i++;
	_kwMap["not_eq"] = i++;
	_kwMap["nullptr"] = i++;
	_kwMap["operator"] = i++;
	_kwMap["or"] = i++;
	_kwMap["or_eq"] = i++;
	_kwMap["private"] = i++;
	_kwMap["protected"] = i++;
	_kwMap["public"] = i++;
	_kwMap["register"] = i++;
	_kwMap["reinterpret_cast"] = i++;
	_kwMap["return"] = i++;
	_kwMap["short"] = i++;
	_kwMap["signed"] = i++;
	_kwMap["sizeof"] = i++;
	_kwMap["static"] = i++;
	_kwMap["static_assert"] = i++;
	_kwMap["static_cast"] = i++;
	_kwMap["struct"] = i++;
	_kwMap["switch"] = i++;
	_kwMap["template"] = i++;
	_kwMap["this"] = i++;
	_kwMap["thread_local"] = i++;
	_kwMap["throw"] = i++;
	_kwMap["true"] = i++;
	_kwMap["try"] = i++;
	_kwMap["typedef"] = i++;
	_kwMap["typeid"] = i++;
	_kwMap["typename"] = i++;
	_kwMap["union"] = i++;
	_kwMap["unsigned"] = i++;
	_kwMap["using"] = i++;
	_kwMap["virtual"] = i++;
	_kwMap["void"] = i++;
	_kwMap["volatile"] = i++;
	_kwMap["wchar_t"] = i++;
	_kwMap["while"] = i++;
	_kwMap["xor"] = i++;
	_kwMap["xor_eq"] = i++;
}


IdentifierToken::~IdentifierToken()
{
}


Token::Class IdentifierToken::tokenClass() const
{
	return asInteger() ? Token::KEYWORD_TOKEN : Token::IDENTIFIER_TOKEN;
}


bool IdentifierToken::start(char c, std::istream& istr)
{
	_value = c;
	return (c >= 'A' && c <= 'Z') ||
	       (c >= 'a' && c <= 'z') ||
	       (c == '_' || c == '$');
}


void IdentifierToken::finish(std::istream& istr)
{
	int next = (char) istr.peek();
	while ((next >= 'A' && next <= 'Z') ||
		   (next >= 'a' && next <= 'z') ||
		   (next >= '0' && next <= '9') ||
		   (next == '_' || next == '$')) 
	{
		_value += (char) istr.get();
		next = istr.peek();
	}
}


int IdentifierToken::asInteger() const
{
	KWMap::const_iterator it = _kwMap.find(_value);
	if (it != _kwMap.end())
		return it->second;
	else
		return 0;
}


StringLiteralToken::StringLiteralToken()
{
}


StringLiteralToken::~StringLiteralToken()
{
}


Token::Class StringLiteralToken::tokenClass() const
{
	return Token::STRING_LITERAL_TOKEN;
}


bool StringLiteralToken::start(char c, std::istream& istr)
{
	_value = c;
	return c == '"';
}


void StringLiteralToken::finish(std::istream& istr)
{
	int next = istr.peek();	
	while (next != -1 && next != '"' && next != '\n' && next != '\r')
	{
		if (next == '\\') _value += (char) istr.get();
		_value += (char) istr.get();
		next = istr.peek();
	}
	if (next == '"')
	{
		next = istr.get();
		_value += (char) next;
	}
	else throw SyntaxException("Unterminated string literal");
}


std::string StringLiteralToken::asString() const
{
	std::string result;
	std::string::const_iterator it  = _value.begin();
	std::string::const_iterator end = _value.end();
	if (it != end)
	{
		if (*it == '"') ++it;
		while (it != end && *it != '"')
		{
			if (*it == '\\') ++it;
			if (it != end) result += *it++;
		}
	}
	return result;
}


CharLiteralToken::CharLiteralToken()
{
}


CharLiteralToken::~CharLiteralToken()
{
}


Token::Class CharLiteralToken::tokenClass() const
{
	return Token::CHAR_LITERAL_TOKEN;
}


bool CharLiteralToken::start(char c, std::istream& istr)
{
	_value = c;
	return c == '\'';
}


void CharLiteralToken::finish(std::istream& istr)
{
	int next = istr.peek();	
	while (next != -1 && next != '\'' && next != '\n' && next != '\r')
	{
		if (next == '\\') _value += (char) istr.get();
		_value += (char) istr.get();
		next = istr.peek();
	}
	if (next == '\'')
	{
		next = istr.get();
		_value += (char) next;
	}
	else throw SyntaxException("Unterminated character literal");
}


char CharLiteralToken::asChar() const
{
	char result('\0');
	std::string::const_iterator it  = _value.begin();
	std::string::const_iterator end = _value.end();
	if (it != end)
	{
		if (*it == '\'') ++it;
		while (it != end && *it != '\'')
		{
			if (*it == '\\') ++it;
			if (it != end) result = *it++;
		}
	}
	return result;
}


NumberLiteralToken::NumberLiteralToken():
	_isFloat(false)
{
}


NumberLiteralToken::~NumberLiteralToken()
{
}


Token::Class NumberLiteralToken::tokenClass() const
{
	return _isFloat ? Token::FLOAT_LITERAL_TOKEN : Token::INTEGER_LITERAL_TOKEN;
}


bool NumberLiteralToken::start(char c, std::istream& istr)
{
	_value = c;
	int next = istr.peek();
	return (c >= '0' && c <= '9') ||
	       (c == '.' && next >= '0' && next <= '9');
}


void NumberLiteralToken::finish(std::istream& istr)
{
	int next = istr.peek();
	_isFloat = false;
	if (_value[0] != '.') // starts with digit
	{
		if (next == 'x')
		{
			_value += (char) istr.get();
			next = istr.peek();
			while (std::isxdigit(next)) 
			{ 
				_value += (char) istr.get(); 
				next = istr.peek(); 
			}
			while (next == 'L' || next == 'l' || next == 'U' || next == 'u')
			{
				_value += (char) istr.get(); 
				next = istr.peek();
			}
			return;
		}
		while (next >= '0' && next <= '9')
		{
			_value += (char) istr.get();
			next = istr.peek();
		}
		if (next == '.')
		{
			next = istr.get();
			next = istr.peek();
			if (next != '.')
			{
				_isFloat = true;
				_value += '.';
			}
			else // double period
			{
				istr.unget();
			}
		}
	}
	else
	{
		_isFloat = true;
		_value += istr.get();
		next = istr.peek();
	}
	while (next >= '0' && next <= '9')
	{
		_value += (char) istr.get();
		next = istr.peek();
	}
	if (next == 'e' || next == 'E')
	{
		_isFloat = true;
		_value += (char) istr.get();
		next = istr.peek();
		if (next == '+' || next == '-')
		{
			_value += (char) istr.get();
			next = istr.peek();
		}
		if (next >= '0' && next <= '9')
		{
			while (next >= '0' && next <= '9')
			{
				_value += (char) istr.get();
				next = istr.peek();
			}
		}
		else
		{
			std::string s(1, (char) next);
			syntaxError("digit", s);
		}
	}
	if (_isFloat)
	{
		if (next == 'L' || next == 'l' || next == 'F' || next == 'f')
			_value += (char) istr.get(); 
	}
	else
	{
		while (next == 'L' || next == 'l' || next == 'U' || next == 'u')
		{
			_value += (char) istr.get(); 
			next = istr.peek();
		}
	}
}


int NumberLiteralToken::asInteger() const
{
	return static_cast<int>(std::strtol(_value.c_str(), 0, 0));
	
}


double NumberLiteralToken::asFloat() const
{
	return std::strtod(_value.c_str(), 0);
}


CommentToken::CommentToken()
{
}


CommentToken::~CommentToken()
{
}


Token::Class CommentToken::tokenClass() const
{
	return (_value.length() > 2 && _value[2] == '/') ? Token::SPECIAL_COMMENT_TOKEN : Token::COMMENT_TOKEN;
}


bool CommentToken::start(char c, std::istream& istr)
{
	_value = c;
	int next  = istr.peek();
	return c == '/' && (next == '*' || next == '/');
}


void CommentToken::finish(std::istream& istr)
{
	int next = istr.peek();
	if (next == '/')
	{
		while (next != -1 && next != '\r' && next != '\n')
		{
			_value += (char) istr.get();
			next = istr.peek();
		}
	}
	else
	{
		_value += (char) istr.get(); // *
		next = istr.peek();
		while (next != -1)
		{
			next = istr.get();
			_value += (char) next;
			if (next == '*' && istr.peek() == '/') 
			{
				_value += (char) istr.get();
				break;
			}
		}
	}
}


std::string CommentToken::asString() const
{
	if (_value.length() > 2 && _value[2] == '/')
		return _value.substr(3);
	else
		return _value.substr(2);
}


PreprocessorToken::PreprocessorToken()
{
}


PreprocessorToken::~PreprocessorToken()
{
}


Token::Class PreprocessorToken::tokenClass() const
{
	return Token::PREPROCESSOR_TOKEN;
}


bool PreprocessorToken::start(char c, std::istream& istr)
{
	_value = c;
	return c == '#';
}


void PreprocessorToken::finish(std::istream& istr)
{
	int pb = -1;
	int next = istr.peek();
	while (next != -1 && next != '\r' && next != '\n')
	{
		if (next == '\\') 
		{
			istr.get();
			int p = istr.peek();
			if (p == '\r')
			{
				istr.get();
				if (istr.peek() == '\n')
					p = istr.get();
				next = p;
			}
			else if (p == '\n')
			{
				next = p;
			}
			else
			{
				pb = next;
			}
		}
		if (next != -1)
		{
			_value += (char) (pb != -1 ? pb : istr.get());
			next = istr.peek();
			pb = -1;
		}
	}
}


} } // namespace Poco::CppParser

