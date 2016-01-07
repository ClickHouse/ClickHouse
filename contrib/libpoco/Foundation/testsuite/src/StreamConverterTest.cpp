//
// StreamConverterTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/StreamConverterTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "StreamConverterTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/StreamConverter.h"
#include "Poco/ASCIIEncoding.h"
#include "Poco/Latin1Encoding.h"
#include "Poco/UTF8Encoding.h"
#include "Poco/StreamCopier.h"
#include <sstream>


using Poco::InputStreamConverter;
using Poco::OutputStreamConverter;
using Poco::Latin1Encoding;
using Poco::UTF8Encoding;
using Poco::ASCIIEncoding;
using Poco::StreamCopier;


StreamConverterTest::StreamConverterTest(const std::string& name): CppUnit::TestCase(name)
{
}


StreamConverterTest::~StreamConverterTest()
{
}


void StreamConverterTest::testIdentityASCIIIn()
{
	ASCIIEncoding encoding;
	
	std::istringstream istr1("");
	std::ostringstream ostr1;
	InputStreamConverter converter1(istr1, encoding, encoding);
	StreamCopier::copyStream(converter1, ostr1);
	assert (ostr1.str() == "");
	assert (converter1.errors() == 0);
	
	std::istringstream istr2("foo bar");
	std::ostringstream ostr2;
	InputStreamConverter converter2(istr2, encoding, encoding);
	StreamCopier::copyStream(converter2, ostr2);
	assert (ostr2.str() == "foo bar");
	assert (converter2.errors() == 0);

	std::istringstream istr3("x");
	std::ostringstream ostr3;
	InputStreamConverter converter3(istr3, encoding, encoding);
	StreamCopier::copyStream(converter3, ostr3);
	assert (ostr3.str() == "x");
	assert (converter3.errors() == 0);
}


void StreamConverterTest::testIdentityASCIIOut()
{
	ASCIIEncoding encoding;
	
	std::ostringstream ostr1;
	OutputStreamConverter converter1(ostr1, encoding, encoding);
	converter1 << "";
	assert (ostr1.str() == "");
	assert (converter1.errors() == 0);
	
	std::ostringstream ostr2;
	OutputStreamConverter converter2(ostr2, encoding, encoding);
	converter2 << "foo bar";
	assert (ostr2.str() == "foo bar");
	assert (converter2.errors() == 0);

	std::ostringstream ostr3;
	OutputStreamConverter converter3(ostr3, encoding, encoding);
	converter3 << "x";
	assert (ostr3.str() == "x");
	assert (converter3.errors() == 0);
}


void StreamConverterTest::testIdentityUTF8In()
{
	UTF8Encoding encoding;
	
	std::istringstream istr1("");
	std::ostringstream ostr1;
	InputStreamConverter converter1(istr1, encoding, encoding);
	StreamCopier::copyStream(converter1, ostr1);
	assert (ostr1.str() == "");
	assert (converter1.errors() == 0);
	
	std::istringstream istr2("foo bar");
	std::ostringstream ostr2;
	InputStreamConverter converter2(istr2, encoding, encoding);
	StreamCopier::copyStream(converter2, ostr2);
	assert (ostr2.str() == "foo bar");
	assert (converter2.errors() == 0);

	std::istringstream istr3("x");
	std::ostringstream ostr3;
	InputStreamConverter converter3(istr3, encoding, encoding);
	StreamCopier::copyStream(converter3, ostr3);
	assert (ostr3.str() == "x");
	assert (converter3.errors() == 0);
	
	const unsigned char greek[] = {0x20, 0xce, 0xba, 0xe1, 0xbd, 0xb9, 0xcf, 0x83, 0xce, 0xbc, 0xce, 0xb5, 0x20, 0x00};
	std::string text((const char*) greek);

	std::istringstream istr4(text);
	std::ostringstream ostr4;
	InputStreamConverter converter4(istr4, encoding, encoding);
	StreamCopier::copyStream(converter4, ostr4);
	assert (ostr4.str() == text);
	assert (converter4.errors() == 0);

	const unsigned char supp[] = {0x41, 0x42, 0xf0, 0x90, 0x82, 0xa4, 0xf0, 0xaf, 0xa6, 0xa0, 0xf0, 0xaf, 0xa8, 0x9d, 0x00};
	std::string text2((const char*) supp);

	std::istringstream istr5(text2);
	std::ostringstream ostr5;
	InputStreamConverter converter5(istr5, encoding, encoding);
	StreamCopier::copyStream(converter5, ostr5);
	assert (ostr5.str() == text2);
	assert (converter5.errors() == 0);


}


void StreamConverterTest::testIdentityUTF8Out()
{
	UTF8Encoding encoding;
	
	std::ostringstream ostr1;
	OutputStreamConverter converter1(ostr1, encoding, encoding);
	converter1 << "";
	assert (ostr1.str() == "");
	assert (converter1.errors() == 0);
	
	std::ostringstream ostr2;
	OutputStreamConverter converter2(ostr2, encoding, encoding);
	converter2 << "foo bar";
	assert (ostr2.str() == "foo bar");
	assert (converter2.errors() == 0);

	std::ostringstream ostr3;
	OutputStreamConverter converter3(ostr3, encoding, encoding);
	converter3 << "x";
	assert (ostr3.str() == "x");
	assert (converter3.errors() == 0);
	
	const unsigned char greek[] = {0x20, 0xce, 0xba, 0xe1, 0xbd, 0xb9, 0xcf, 0x83, 0xce, 0xbc, 0xce, 0xb5, 0x20, 0x00};
	std::string text((const char*) greek);

	std::ostringstream ostr4;
	OutputStreamConverter converter4(ostr4, encoding, encoding);
	converter4 << text;
	assert (ostr4.str() == text);
	assert (converter4.errors() == 0);
}


void StreamConverterTest::testUTF8toASCIIIn()
{
	UTF8Encoding utf8Encoding;
	ASCIIEncoding asciiEncoding;

	const unsigned char greek[] = {0x20, 0xce, 0xba, 0xe1, 0xbd, 0xb9, 0xcf, 0x83, 0xce, 0xbc, 0xce, 0xb5, 0x20, 0x41, 0x42, 0x00};
	std::string text((const char*) greek);

	std::istringstream istr1(text);
	std::ostringstream ostr1;
	InputStreamConverter converter1(istr1, utf8Encoding, asciiEncoding);
	StreamCopier::copyStream(converter1, ostr1);
	assert (ostr1.str() == " ????? AB");
	assert (converter1.errors() == 0);

	std::istringstream istr2("abcde");
	std::ostringstream ostr2;
	InputStreamConverter converter2(istr2, utf8Encoding, asciiEncoding);
	StreamCopier::copyStream(converter2, ostr2);
	assert (ostr2.str() == "abcde");
	assert (converter2.errors() == 0);
}


void StreamConverterTest::testUTF8toASCIIOut()
{
	UTF8Encoding utf8Encoding;
	ASCIIEncoding asciiEncoding;

	const unsigned char greek[] = {0x20, 0xce, 0xba, 0xe1, 0xbd, 0xb9, 0xcf, 0x83, 0xce, 0xbc, 0xce, 0xb5, 0x20, 0x41, 0x42, 0x00};
	std::string text((const char*) greek);

	std::ostringstream ostr1;
	OutputStreamConverter converter1(ostr1, utf8Encoding, asciiEncoding);
	converter1 << text;
	assert (ostr1.str() == " ????? AB");
	assert (converter1.errors() == 0);

	std::ostringstream ostr2;
	OutputStreamConverter converter2(ostr2, utf8Encoding, asciiEncoding);
	converter2 << "abcde";
	assert (ostr2.str() == "abcde");
	assert (converter2.errors() == 0);
}


void StreamConverterTest::testLatin1toUTF8In()
{
	UTF8Encoding utf8Encoding;
	Latin1Encoding latin1Encoding;
	
	const unsigned char latin1Chars[] = {'g', 252, 'n', 't', 'e', 'r', 0};
	const unsigned char utf8Chars[]   = {'g', 195, 188, 'n', 't', 'e', 'r', 0};
	std::string latin1Text((const char*) latin1Chars);
	std::string utf8Text((const char*) utf8Chars);

	std::istringstream istr1(latin1Text);
	std::ostringstream ostr1;
	InputStreamConverter converter1(istr1, latin1Encoding, utf8Encoding);
	StreamCopier::copyStream(converter1, ostr1);
	assert (ostr1.str() == utf8Text);
	assert (converter1.errors() == 0);
}


void StreamConverterTest::testLatin1toUTF8Out()
{
	UTF8Encoding utf8Encoding;
	Latin1Encoding latin1Encoding;
	
	const unsigned char latin1Chars[] = {'g', 252, 'n', 't', 'e', 'r', 0};
	const unsigned char utf8Chars[]   = {'g', 195, 188, 'n', 't', 'e', 'r', 0};
	std::string latin1Text((const char*) latin1Chars);
	std::string utf8Text((const char*) utf8Chars);

	std::ostringstream ostr1;
	OutputStreamConverter converter1(ostr1, latin1Encoding, utf8Encoding);
	converter1 << latin1Text;
	assert (ostr1.str() == utf8Text);
	assert (converter1.errors() == 0);
}


void StreamConverterTest::testErrorsIn()
{
	UTF8Encoding utf8Encoding;
	Latin1Encoding latin1Encoding;

	const unsigned char badChars[] = {'a', 'b', 255, 'c', 254, 0};
	std::string badText((const char*) badChars);
	
	std::istringstream istr1(badText);
	std::ostringstream ostr1;
	InputStreamConverter converter1(istr1, utf8Encoding, latin1Encoding);
	StreamCopier::copyStream(converter1, ostr1);
	assert (converter1.errors() == 2);
}


void StreamConverterTest::testErrorsOut()
{
	UTF8Encoding utf8Encoding;
	Latin1Encoding latin1Encoding;

	const unsigned char badChars[] = {'a', 'b', 255, 'c', 254, 0};
	std::string badText((const char*) badChars);
	
	std::ostringstream ostr1;
	OutputStreamConverter converter1(ostr1, utf8Encoding, latin1Encoding);
	converter1 << badText;
	assert (converter1.errors() == 1);
}


void StreamConverterTest::setUp()
{
}


void StreamConverterTest::tearDown()
{
}


CppUnit::Test* StreamConverterTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("StreamConverterTest");

	CppUnit_addTest(pSuite, StreamConverterTest, testIdentityASCIIIn);
	CppUnit_addTest(pSuite, StreamConverterTest, testIdentityASCIIOut);
	CppUnit_addTest(pSuite, StreamConverterTest, testIdentityUTF8In);
	CppUnit_addTest(pSuite, StreamConverterTest, testIdentityUTF8Out);
	CppUnit_addTest(pSuite, StreamConverterTest, testUTF8toASCIIIn);
	CppUnit_addTest(pSuite, StreamConverterTest, testUTF8toASCIIOut);
	CppUnit_addTest(pSuite, StreamConverterTest, testLatin1toUTF8In);
	CppUnit_addTest(pSuite, StreamConverterTest, testLatin1toUTF8Out);
	CppUnit_addTest(pSuite, StreamConverterTest, testErrorsIn);
	CppUnit_addTest(pSuite, StreamConverterTest, testErrorsOut);

	return pSuite;
}
