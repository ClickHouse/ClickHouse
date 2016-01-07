//
// StreamTokenizerTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/StreamTokenizerTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "StreamTokenizerTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/StreamTokenizer.h"
#include "Poco/Token.h"
#include "Poco/Ascii.h"
#include <sstream>


using Poco::StreamTokenizer;
using Poco::Token;
using Poco::InvalidToken;
using Poco::EOFToken;
using Poco::WhitespaceToken;
using Poco::Ascii;


class IdentifierToken: public Token
{
public:
	IdentifierToken()
	{
	}
	
	~IdentifierToken()
	{
	}

	Class tokenClass() const
	{
		return Token::IDENTIFIER_TOKEN;
	}
	
	bool start(char c, std::istream& istr)
	{
		if (c != -1 && Ascii::isAlpha(c))
		{
			_value = c;
			return true;
		}
		else return false;
	}
	
	void finish(std::istream& istr)
	{
		int c = istr.peek();
		while (c != -1 && Ascii::isAlphaNumeric(c))
		{
			istr.get();
			_value += c;
			c = istr.peek();
		}
	}
};


class IntLiteralToken: public Token
{
public:
	IntLiteralToken()
	{
	}
	
	~IntLiteralToken()
	{
	}

	Class tokenClass() const
	{
		return Token::INTEGER_LITERAL_TOKEN;
	}
	
	bool start(char c, std::istream& istr)
	{
		if (c != -1 && Ascii::isDigit(c))
		{
			_value = c;
			return true;
		}
		else return false;
	}
	
	void finish(std::istream& istr)
	{
		int c = istr.peek();
		while (c != -1 && Ascii::isDigit(c))
		{
			istr.get();
			_value += c;
			c = istr.peek();
		}
	}
};


StreamTokenizerTest::StreamTokenizerTest(const std::string& name): CppUnit::TestCase(name)
{
}


StreamTokenizerTest::~StreamTokenizerTest()
{
}


void StreamTokenizerTest::testTokenizer1()
{
	std::string data = "";
	std::istringstream istr(data);
	StreamTokenizer tokenizer(istr);
	tokenizer.addToken(new WhitespaceToken());
	tokenizer.addToken(new IdentifierToken());
	tokenizer.addToken(new IntLiteralToken());
	
	const Token* next = tokenizer.next();
	assert (next->tokenClass() == Token::EOF_TOKEN);
}


void StreamTokenizerTest::testTokenizer2()
{
	std::string data = "foo";
	std::istringstream istr(data);
	StreamTokenizer tokenizer(istr);
	tokenizer.addToken(new WhitespaceToken());
	tokenizer.addToken(new IdentifierToken());
	tokenizer.addToken(new IntLiteralToken());
	
	const Token* next = tokenizer.next();
	assert (next->tokenClass() == Token::IDENTIFIER_TOKEN);
	assert (next->tokenString() == "foo");
	
	next = tokenizer.next();
	assert (next->tokenClass() == Token::EOF_TOKEN);
}


void StreamTokenizerTest::testTokenizer3()
{
	std::string data = "foo bar";
	std::istringstream istr(data);
	StreamTokenizer tokenizer(istr);
	tokenizer.addToken(new WhitespaceToken());
	tokenizer.addToken(new IdentifierToken());
	tokenizer.addToken(new IntLiteralToken());
	
	const Token* next = tokenizer.next();
	assert (next->tokenClass() == Token::IDENTIFIER_TOKEN);
	assert (next->tokenString() == "foo");

	next = tokenizer.next();
	assert (next->tokenClass() == Token::IDENTIFIER_TOKEN);
	assert (next->tokenString() == "bar");

	next = tokenizer.next();
	assert (next->tokenClass() == Token::EOF_TOKEN);
}


void StreamTokenizerTest::testTokenizer4()
{
	std::string data = "foo   123";
	std::istringstream istr(data);
	StreamTokenizer tokenizer(istr);
	tokenizer.addToken(new WhitespaceToken());
	tokenizer.addToken(new IdentifierToken());
	tokenizer.addToken(new IntLiteralToken());
	
	const Token* next = tokenizer.next();
	assert (next->tokenClass() == Token::IDENTIFIER_TOKEN);
	assert (next->tokenString() == "foo");

	next = tokenizer.next();
	assert (next->tokenClass() == Token::INTEGER_LITERAL_TOKEN);
	assert (next->asInteger() == 123);

	next = tokenizer.next();
	assert (next->tokenClass() == Token::EOF_TOKEN);
}


void StreamTokenizerTest::testTokenizer5()
{
	std::string data = "foo # 123";
	std::istringstream istr(data);
	StreamTokenizer tokenizer(istr);
	tokenizer.addToken(new WhitespaceToken());
	tokenizer.addToken(new IdentifierToken());
	tokenizer.addToken(new IntLiteralToken());
	
	const Token* next = tokenizer.next();
	assert (next->tokenClass() == Token::IDENTIFIER_TOKEN);
	assert (next->tokenString() == "foo");

	next = tokenizer.next();
	assert (next->tokenClass() == Token::INVALID_TOKEN);
	assert (next->tokenString() == "#");

	next = tokenizer.next();
	assert (next->tokenClass() == Token::INTEGER_LITERAL_TOKEN);
	assert (next->asInteger() == 123);

	next = tokenizer.next();
	assert (next->tokenClass() == Token::EOF_TOKEN);
}


void StreamTokenizerTest::testTokenizer6()
{
	std::string data = "foo 123 #";
	std::istringstream istr(data);
	StreamTokenizer tokenizer(istr);
	tokenizer.addToken(new WhitespaceToken());
	tokenizer.addToken(new IdentifierToken());
	tokenizer.addToken(new IntLiteralToken());
	
	const Token* next = tokenizer.next();
	assert (next->tokenClass() == Token::IDENTIFIER_TOKEN);
	assert (next->tokenString() == "foo");

	next = tokenizer.next();
	assert (next->tokenClass() == Token::INTEGER_LITERAL_TOKEN);
	assert (next->asInteger() == 123);

	next = tokenizer.next();
	assert (next->tokenClass() == Token::INVALID_TOKEN);
	assert (next->tokenString() == "#");

	next = tokenizer.next();
	assert (next->tokenClass() == Token::EOF_TOKEN);
}


void StreamTokenizerTest::testTokenizer7()
{
	std::string data = "  foo 123   ";
	std::istringstream istr(data);
	StreamTokenizer tokenizer(istr);
	tokenizer.addToken(new WhitespaceToken());
	tokenizer.addToken(new IdentifierToken());
	tokenizer.addToken(new IntLiteralToken());
	
	const Token* next = tokenizer.next();
	assert (next->tokenClass() == Token::IDENTIFIER_TOKEN);
	assert (next->tokenString() == "foo");

	next = tokenizer.next();
	assert (next->tokenClass() == Token::INTEGER_LITERAL_TOKEN);
	assert (next->asInteger() == 123);

	next = tokenizer.next();
	assert (next->tokenClass() == Token::EOF_TOKEN);
}


void StreamTokenizerTest::setUp()
{
}


void StreamTokenizerTest::tearDown()
{
}


CppUnit::Test* StreamTokenizerTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("StreamTokenizerTest");

	CppUnit_addTest(pSuite, StreamTokenizerTest, testTokenizer1);
	CppUnit_addTest(pSuite, StreamTokenizerTest, testTokenizer2);
	CppUnit_addTest(pSuite, StreamTokenizerTest, testTokenizer3);
	CppUnit_addTest(pSuite, StreamTokenizerTest, testTokenizer4);
	CppUnit_addTest(pSuite, StreamTokenizerTest, testTokenizer5);
	CppUnit_addTest(pSuite, StreamTokenizerTest, testTokenizer6);
	CppUnit_addTest(pSuite, StreamTokenizerTest, testTokenizer7);

	return pSuite;
}
