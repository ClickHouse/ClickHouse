//
// UTF8StringTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/UTF8StringTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "UTF8StringTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/UTF8String.h"


using Poco::UTF8;


UTF8StringTest::UTF8StringTest(const std::string& name): CppUnit::TestCase(name)
{
}


UTF8StringTest::~UTF8StringTest()
{
}


void UTF8StringTest::testCompare()
{
	std::string a1("aaaaa");
	std::string b1("bbbbb");
	
	assert (UTF8::icompare(a1, b1) < 0);

	std::string a2("aaaaa");
	std::string b2("BBBBB");
	
	assert (UTF8::icompare(a2, b2) < 0);

	std::string a3("AAAAA");
	std::string b3("bbbbb");
	
	assert (UTF8::icompare(a3, b3) < 0);

	std::string a4("aaaaa");
	std::string b4("AAAAA");
	
	assert (UTF8::icompare(a4, b4) == 0);
	
	std::string a5("AAAAA");
	std::string b5("bbbbb");
	
	assert (UTF8::icompare(a5, b5) < 0);

	std::string a6("\303\274\303\266\303\244"); // "u"o"a
	std::string b6("\303\234\303\226\303\204"); // "U"O"A
	
	assert (UTF8::icompare(a6, b6) == 0);
}


void UTF8StringTest::testTransform()
{
	std::string s1("abcde");
	UTF8::toUpperInPlace(s1);
	assert (s1 == "ABCDE");

	std::string s2("aBcDe123");
	UTF8::toUpperInPlace(s2);
	assert (s2 == "ABCDE123");

	std::string s3("\303\274\303\266\303\244"); // "u"o"a
	UTF8::toUpperInPlace(s3);	
	assert (s3 == "\303\234\303\226\303\204"); // "U"O"A
	UTF8::toLowerInPlace(s3);
	assert (s3 == "\303\274\303\266\303\244"); // "u"o"a

	// a mix of invalid sequences
	std::string str = "\xC2\xE5\xF0\xF8\xE8\xED\xFB+-++";
	assert ("???" == UTF8::toLower(str));
}


void UTF8StringTest::setUp()
{
}


void UTF8StringTest::tearDown()
{
}


CppUnit::Test* UTF8StringTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("UTF8StringTest");

	CppUnit_addTest(pSuite, UTF8StringTest, testCompare);
	CppUnit_addTest(pSuite, UTF8StringTest, testTransform);

	return pSuite;
}
