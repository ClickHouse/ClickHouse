//
// TextIteratorTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/TextIteratorTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "TextIteratorTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/TextIterator.h"
#include "Poco/Latin1Encoding.h"
#include "Poco/UTF8Encoding.h"
#include "Poco/UTF16Encoding.h"


using Poco::TextIterator;
using Poco::Latin1Encoding;
using Poco::UTF8Encoding;
using Poco::UTF16Encoding;


TextIteratorTest::TextIteratorTest(const std::string& name): CppUnit::TestCase(name)
{
}


TextIteratorTest::~TextIteratorTest()
{
}


void TextIteratorTest::testEmptyLatin1()
{
	Latin1Encoding encoding;
	std::string text;
	TextIterator it(text, encoding);
	TextIterator end(text);
	
	assert (it == end);
}


void TextIteratorTest::testOneLatin1()
{
	Latin1Encoding encoding;
	std::string text("x");
	TextIterator it(text, encoding);
	TextIterator end(text);
	
	assert (it != end);
	assert (*it == 'x');
	++it;
	assert (it == end);
}


void TextIteratorTest::testLatin1()
{
	Latin1Encoding encoding;
	std::string text("Latin1");
	TextIterator it(text, encoding);
	TextIterator end(text);
	
	assert (it != end);
	assert (*it++ == 'L');
	assert (it != end);
	assert (*it++ == 'a');
	assert (it != end);
	assert (*it++ == 't');
	assert (it != end);
	assert (*it++ == 'i');
	assert (it != end);
	assert (*it++ == 'n');
	assert (it != end);
	assert (*it++ == '1');
	assert (it == end);
	
	std::string empty;
	it  = TextIterator(empty, encoding);
	end = TextIterator(empty);
	assert (it == end);
}


void TextIteratorTest::testEmptyUTF8()
{
	UTF8Encoding encoding;
	std::string text;
	TextIterator it(text, encoding);
	TextIterator end(text);
	
	assert (it == end);
}


void TextIteratorTest::testOneUTF8()
{
	UTF8Encoding encoding;
	
	// 1 byte sequence
	std::string text("x");
	TextIterator it(text, encoding);
	TextIterator end(text);
	
	assert (it != end);
	assert (*it == 'x');
	++it;
	assert (it == end);
	
	unsigned char data[Poco::TextEncoding::MAX_SEQUENCE_LENGTH];
	
	// 2 byte sequence
	int n = encoding.convert(0xab, data, sizeof(data));
	assert (n == 2);
	text.assign((char*) data, n);
	it  = TextIterator(text, encoding);
	end = TextIterator(text);
	
	assert (it != end);
	assert (*it++ == 0xab);
	assert (it == end);

	// 3 byte sequence
	n = encoding.convert(0xabcd, data, sizeof(data));
	assert (n == 3);
	text.assign((char*) data, n);
	it  = TextIterator(text, encoding);
	end = TextIterator(text);
	
	assert (it != end);
	assert (*it++ == 0xabcd);
	assert (it == end);

	// 4 byte sequence
	n = encoding.convert(0xabcde, data, sizeof(data));
	assert (n == 4);
	text.assign((char*) data, n);
	it  = TextIterator(text, encoding);
	end = TextIterator(text);
	
	assert (it != end);
	assert (*it++ == 0xabcde);
	assert (it == end);
	
	// 5 byte sequence - not supported
	n = encoding.convert(0xabcdef, data, sizeof(data));
	assert (n == 0);

	// 6 byte sequence - not supported
	n = encoding.convert(0xfabcdef, data, sizeof(data));
	assert (n == 0);
}


void TextIteratorTest::testUTF8()
{
	UTF8Encoding encoding;
	const unsigned char greek[] = {0x20, 0xce, 0xba, 0xe1, 0xbd, 0xb9, 0xcf, 0x83, 0xce, 0xbc, 0xce, 0xb5, 0x20, 0x00};
	std::string text((const char*) greek);
	TextIterator it(text, encoding);
	TextIterator end(text);
	
	assert (it != end);
	assert (*it++ == 0x0020);
	assert (it != end);
	assert (*it++ == 0x03ba);
	assert (it != end);
	assert (*it++ == 0x1f79);
	assert (it != end);
	assert (*it++ == 0x03c3);
	assert (it != end);
	assert (*it++ == 0x03bc);
	assert (it != end);
	assert (*it++ == 0x03b5);
	assert (it != end);
	assert (*it++ == 0x0020);
	assert (it == end);
}


void TextIteratorTest::testUTF8Supplementary()
{
	UTF8Encoding encoding; 
	const unsigned char supp[] = {0x41, 0x42, 0xf0, 0x90, 0x82, 0xa4, 0xf0, 0xaf, 0xa6, 0xa0, 0xf0, 0xaf, 0xa8, 0x9d, 0x00};
	std::string text((const char*) supp);
	TextIterator it(text, encoding);
	TextIterator end(text);
	
	assert (it != end);
	assert (*it++ == 0x0041);
	assert (it != end);
	assert (*it++ == 0x0042);
	assert (it != end);
	assert (*it++ == 0x100a4);
	assert (it != end);
	assert (*it++ == 0x2f9a0);
	assert (it != end);
	assert (*it++ == 0x2fa1d);
	assert (it == end);
}


void TextIteratorTest::testUTF16Supplementary()
{
	UTF16Encoding encoding; 
	const Poco::UInt16 supp [] = { 0x0041, 0x0042, 0xD800, 0xDCA4, 0xD87E, 0xDDA0, 0xD87E, 0xDE1D, 0x00};
	std::string text((const char*) supp, 16);
	TextIterator it(text, encoding);
	TextIterator end(text);
	
	assert (it != end);
	assert (*it++ == 0x0041);
	assert (it != end);
	assert (*it++ == 0x0042);
	assert (it != end);
	assert (*it++ == 0x100a4);
	assert (it != end);
	assert (*it++ == 0x2f9a0);
	assert (it != end);
	assert (*it++ == 0x2fa1d);
	assert (it == end);
}


void TextIteratorTest::testSwap()
{
	Latin1Encoding encoding;
	std::string text("x");
	TextIterator it1(text, encoding);
	TextIterator it2(text, encoding);
	TextIterator end(text);
	
	assert (it1 == it2);
	it2.swap(end);
	assert (it1 != it2);
	it2.swap(end);
	assert (it1 == it2);
}


void TextIteratorTest::setUp()
{
}


void TextIteratorTest::tearDown()
{
}


CppUnit::Test* TextIteratorTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("TextIteratorTest");

	CppUnit_addTest(pSuite, TextIteratorTest, testEmptyLatin1);
	CppUnit_addTest(pSuite, TextIteratorTest, testOneLatin1);
	CppUnit_addTest(pSuite, TextIteratorTest, testLatin1);
	CppUnit_addTest(pSuite, TextIteratorTest, testEmptyUTF8);
	CppUnit_addTest(pSuite, TextIteratorTest, testOneUTF8);
	CppUnit_addTest(pSuite, TextIteratorTest, testUTF8);
	CppUnit_addTest(pSuite, TextIteratorTest, testUTF8Supplementary);
	CppUnit_addTest(pSuite, TextIteratorTest, testUTF16Supplementary);
	CppUnit_addTest(pSuite, TextIteratorTest, testSwap);

	return pSuite;
}
