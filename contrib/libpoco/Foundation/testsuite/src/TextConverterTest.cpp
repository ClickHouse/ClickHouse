//
// TextConverterTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/TextConverterTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "TextConverterTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/TextConverter.h"
#include "Poco/ASCIIEncoding.h"
#include "Poco/Latin1Encoding.h"
#include "Poco/Latin2Encoding.h"
#include "Poco/Latin9Encoding.h"
#include "Poco/Windows1250Encoding.h"
#include "Poco/Windows1251Encoding.h"
#include "Poco/Windows1252Encoding.h"
#include "Poco/UTF8Encoding.h"


using namespace Poco;


TextConverterTest::TextConverterTest(const std::string& name): CppUnit::TestCase(name)
{
}


TextConverterTest::~TextConverterTest()
{
}


void TextConverterTest::testIdentityASCII()
{
	ASCIIEncoding encoding;
	TextConverter converter(encoding, encoding);
	
	std::string empty;
	std::string result0;
	int errors = converter.convert(empty, result0);
	assert (result0 == empty);
	assert (errors == 0);
	
	std::string fooBar = "foo bar";
	std::string result1;
	errors = converter.convert(fooBar, result1);
	assert (result1 == fooBar);
	assert (errors == 0);

	std::string result2;
	errors = converter.convert(fooBar.data(), (int) fooBar.length(), result2);
	assert (result2 == fooBar);
	assert (errors == 0);

	std::string result3;
	errors = converter.convert("", 0, result3);
	assert (result3.empty());
	assert (errors == 0);
	
	std::string x = "x";
	std::string result4;
	errors = converter.convert(x, result4);
	assert (result4 == x);
	assert (errors == 0);

	std::string result5;
	errors = converter.convert("x", 1, result5);
	assert (result5 == x);
	assert (errors == 0);
}


void TextConverterTest::testIdentityUTF8()
{
	UTF8Encoding encoding;
	TextConverter converter(encoding, encoding);
	
	std::string empty;
	std::string result0;
	int errors = converter.convert(empty, result0);
	assert (result0 == empty);
	assert (errors == 0);
	
	std::string fooBar = "foo bar";
	std::string result1;
	errors = converter.convert(fooBar, result1);
	assert (result1 == fooBar);
	assert (errors == 0);

	std::string result2;
	errors = converter.convert(fooBar.data(), (int) fooBar.length(), result2);
	assert (result2 == fooBar);
	assert (errors == 0);

	std::string result3;
	errors = converter.convert("", 0, result3);
	assert (result3.empty());
	assert (errors == 0);
	
	const unsigned char greek[] = {0x20, 0xce, 0xba, 0xe1, 0xbd, 0xb9, 0xcf, 0x83, 0xce, 0xbc, 0xce, 0xb5, 0x20, 0x00};
	std::string text((const char*) greek);
	
	std::string result4;
	errors = converter.convert(text, result4);
	assert (result4 == text);
	assert (errors == 0);
	
	std::string result5;
	errors = converter.convert((char*) greek, 13, result5);
	assert (result5 == text);
	assert (errors == 0);
	
	std::string x = "x";
	std::string result6;
	errors = converter.convert(x, result6);
	assert (result6 == x);
	assert (errors == 0);

	std::string result7;
	errors = converter.convert("x", 1, result7);
	assert (result7 == x);
	assert (errors == 0);
	
	std::string utfChar((char*) greek + 1, 2);
	std::string result8;
	errors = converter.convert(utfChar, result8);
	assert (result8 == utfChar);
	assert (errors == 0);
	
	std::string result9;
	errors = converter.convert((char*) greek + 1, 2, result9);
	assert (result9 == utfChar);
	assert (errors == 0);
}


void TextConverterTest::testUTF8toASCII()
{
	UTF8Encoding utf8Encoding;
	ASCIIEncoding asciiEncoding;
	TextConverter converter(utf8Encoding, asciiEncoding);

	const unsigned char greek[] = {0x20, 0xce, 0xba, 0xe1, 0xbd, 0xb9, 0xcf, 0x83, 0xce, 0xbc, 0xce, 0xb5, 0x20, 0x41, 0x42, 0x00};
	std::string text((const char*) greek);
	std::string result0;
	int errors = converter.convert(text, result0);
	assert (result0 == " ????? AB");
	assert (errors == 0);
	
	std::string result1;
	errors = converter.convert("abcde", 5, result1);
	assert (result1 == "abcde");
}


void TextConverterTest::testLatin1toUTF8()
{
	Latin1Encoding latin1Encoding;
	UTF8Encoding utf8Encoding;
	TextConverter converter(latin1Encoding, utf8Encoding);
	
	const unsigned char latin1Chars[] = {'g', 252, 'n', 't', 'e', 'r', 0};
	const unsigned char utf8Chars[]   = {'g', 195, 188, 'n', 't', 'e', 'r', 0};
	std::string latin1Text((const char*) latin1Chars);
	std::string utf8Text((const char*) utf8Chars);
	
	std::string result0;
	int errors = converter.convert(latin1Text, result0);
	assert (result0 == utf8Text);
	assert (errors == 0);
	assertEqual((long) result0.size(), 7);
	
	std::string result1;
	errors = converter.convert(latin1Chars, 6, result1);
	assert (result1 == utf8Text);
	assert (errors == 0);
}


void TextConverterTest::testLatin2toUTF8()
{
	Latin2Encoding latinEncoding;
	UTF8Encoding utf8Encoding;
	TextConverter converter(latinEncoding, utf8Encoding);

	const unsigned char latinChars[26] = { 	0xb5, 0xb9, 0xe8, 0xbb, 0xbe, 0xfd, 0xe1, 0xed, 0xe9, 0xfa, 0xe4, 0xf4,
									  0x20, 0xa5, 0xa9, 0xc8, 0xab, 0xae, 0xdd, 0xc1, 0xcd, 0xc9, 0xda, 0xc4, 0xd4, 0x00 };
	const unsigned char utf8Chars[] = "ľščťžýáíéúäô ĽŠČŤŽÝÁÍÉÚÄÔ";
	std::string latinText((const char*) latinChars);
	std::string utf8Text((const char*) utf8Chars);

	std::string result0;
	int errors = converter.convert(latinText, result0);
	assertEqual (result0, utf8Text);
	assertEqual (errors, 0);
	assertEqual((long) result0.size(), 49);

	std::string result1;
	errors = converter.convert(latinChars, 25, result1);
	assertEqual (result1, utf8Text);
	assertEqual (errors, 0);
	assertEqual((long) result1.size(), 49);
}


void TextConverterTest::testLatin9toUTF8()
{
	Latin9Encoding latinEncoding;
	UTF8Encoding utf8Encoding;
	TextConverter converter(latinEncoding, utf8Encoding);

	const unsigned char latinChars[26] = { 	0x3f, 0xa8, 0x3f, 0x3f, 0xb8, 0xfd, 0xe1, 0xed, 0xe9, 0xfa, 0xe4, 0xf4,
									  0x20, 0x3f, 0xa6, 0x3f, 0x3f, 0xb4, 0xdd, 0xc1, 0xcd, 0xc9, 0xda, 0xc4, 0xd4, 0x00 };
	const unsigned char utf8Chars[] = "?š??žýáíéúäô ?Š??ŽÝÁÍÉÚÄÔ";
	std::string latinText((const char*) latinChars);
	std::string utf8Text((const char*) utf8Chars);

	std::string result0;
	int errors = converter.convert(latinText, result0);
	assertEqual (result0, utf8Text);
	assertEqual (errors, 0);
	assertEqual((long) result0.size(), 43);

	std::string result1;
	errors = converter.convert(latinChars, 25, result1);
	assertEqual(result1, utf8Text);
	assertEqual((long) errors, 0);
	assertEqual((long) result1.size(), 43);
}


void TextConverterTest::testCP1250toUTF8()
{
	Windows1250Encoding latinEncoding;
	UTF8Encoding utf8Encoding;
	TextConverter converter(latinEncoding, utf8Encoding);

	const unsigned char latinChars[26] = { 	0xbe, 0x9a, 0xe8, 0x9d, 0x9e, 0xfd, 0xe1, 0xed, 0xe9, 0xfa, 0xe4, 0xf4,
									  0x20, 0xbc, 0x8a, 0xc8, 0x8d, 0x8e, 0xdd, 0xc1, 0xcd, 0xc9, 0xda, 0xc4, 0xd4, 0x00 };
	const unsigned char utf8Chars[] = "ľščťžýáíéúäô ĽŠČŤŽÝÁÍÉÚÄÔ";
	std::string latinText((const char*) latinChars);
	std::string utf8Text((const char*) utf8Chars);

	std::string result0;
	int errors = converter.convert(latinText, result0);
	assertEqual (result0, utf8Text);
	assertEqual (errors, 0);
	assertEqual((long) result0.size(), 49);

	std::string result1;
	errors = converter.convert(latinChars, 25, result1);
	assertEqual(result1, utf8Text);
	assertEqual((long) errors, 0);
	assertEqual((long) result1.size(), 49);
}


void TextConverterTest::testCP1251toUTF8()
{
	Windows1251Encoding latinEncoding;
	UTF8Encoding utf8Encoding;
	TextConverter converter(latinEncoding, utf8Encoding);

	const unsigned char latinChars[32] = { 	0xe1, 0xe2, 0xe3, 0xe4, 0xe5, 0xe6, 0xe7, 0xe8, 0xe9, 0xea, 0xeb, 0xec, 0xed, 0xee, 0xef, 0xf0, 0xf1, 0xf2, 0xf3, 0xf4, 0xf5, 0xf6, 0xf7, 0xf8, 0xf9, 0xfa, 0xfb, 0xfc, 0xfd, 0xfe, 0xff, 0x00 };
	const unsigned char utf8Chars[] = "бвгдежзийклмнопрстуфхцчшщъыьэюя";
	std::string latinText((const char*) latinChars);
	std::string utf8Text((const char*) utf8Chars);

	std::string result0;
	int errors = converter.convert(latinText, result0);
	assertEqual (result0, utf8Text);
	assertEqual (errors, 0);
	assertEqual((long) result0.size(), 62);

	std::string result1;
	errors = converter.convert(latinChars, 31, result1);
	assertEqual (result1, utf8Text);
	assertEqual (errors, 0);
	assertEqual((long) result1.size(), 62);
}


void TextConverterTest::testCP1252toUTF8()
{
	Windows1252Encoding latinEncoding;
	UTF8Encoding utf8Encoding;
	TextConverter converter(latinEncoding, utf8Encoding);

	const unsigned char latinChars[26] = { 	0x3f, 0x9a, 0x3f, 0x3f, 0x9e, 0xfd, 0xe1, 0xed, 0xe9, 0xfa, 0xe4, 0xf4,
									  0x20, 0x3f, 0x8a, 0x3f, 0x3f, 0x8e, 0xdd, 0xc1, 0xcd, 0xc9, 0xda, 0xc4, 0xd4, 0x00 };
	const unsigned char utf8Chars[] = "?š??žýáíéúäô ?Š??ŽÝÁÍÉÚÄÔ";
	std::string latinText((const char*) latinChars);
	std::string utf8Text((const char*) utf8Chars);

	std::string result0;
	int errors = converter.convert(latinText, result0);
	assertEqual(result0, utf8Text);
	assertEqual(errors, 0);
	assertEqual((long) result0.size(), 43);

	std::string result1;
	errors = converter.convert(latinChars, 25, result1);
	assertEqual(result1, utf8Text);
	assertEqual(errors, 0);
	assertEqual((long) result1.size(), 43);
}


void TextConverterTest::testErrors()
{
	UTF8Encoding utf8Encoding;
	Latin1Encoding latin1Encoding;
	TextConverter converter(utf8Encoding, latin1Encoding);

	const unsigned char badChars[] = {'a', 'b', 255, 'c', 254, 0};
	std::string badText((const char*) badChars);
	
	std::string result;
	int errors = converter.convert(badText, result);
	assert (errors == 2);
}


void TextConverterTest::setUp()
{
}


void TextConverterTest::tearDown()
{
}


CppUnit::Test* TextConverterTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("TextConverterTest");

	CppUnit_addTest(pSuite, TextConverterTest, testIdentityASCII);
	CppUnit_addTest(pSuite, TextConverterTest, testIdentityUTF8);
	CppUnit_addTest(pSuite, TextConverterTest, testUTF8toASCII);
	CppUnit_addTest(pSuite, TextConverterTest, testLatin1toUTF8);
	CppUnit_addTest(pSuite, TextConverterTest, testLatin2toUTF8);
	CppUnit_addTest(pSuite, TextConverterTest, testLatin9toUTF8);
	CppUnit_addTest(pSuite, TextConverterTest, testCP1250toUTF8);
	CppUnit_addTest(pSuite, TextConverterTest, testCP1251toUTF8);
	CppUnit_addTest(pSuite, TextConverterTest, testCP1252toUTF8);
	CppUnit_addTest(pSuite, TextConverterTest, testErrors);

	return pSuite;
}
