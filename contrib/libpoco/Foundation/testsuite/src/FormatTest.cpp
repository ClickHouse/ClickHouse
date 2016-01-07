//
// FormatTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/FormatTest.cpp#2 $
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// All rights reserved.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "FormatTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Any.h"
#include "Poco/Format.h"
#include "Poco/Exception.h"


using Poco::format;
using Poco::BadCastException;
using Poco::Int64;
using Poco::UInt64;


FormatTest::FormatTest(const std::string& name): CppUnit::TestCase(name)
{
}


FormatTest::~FormatTest()
{
}


void FormatTest::testChar()
{
	char c = 'a';
	std::string s(format("%c", c));
	assert(s == "a");
	s = format("%2c", c);
	assert(s == " a");
	s = format("%-2c", c);
	assert(s == "a ");

	s = format("%c", std::string("foo"));
	assert(s == "[ERRFMT]");
}


void FormatTest::testInt()
{
	int i = 42;
	std::string s(format("%d", i));
	assert (s == "42");
	s = format("%4d", i);
	assert (s == "  42");
	s = format("%04d", i);
	assert (s == "0042");

	short h = 42;
	s = format("%hd", h);
	assert (s == "42");
	s = format("%4hd", h);
	assert (s == "  42");
	s = format("%04hd", h);
	assert (s == "0042");

	unsigned short hu = 42;
	s = format("%hu", hu);
	assert (s == "42");
	s = format("%4hu", hu);
	assert (s == "  42");
	s = format("%04hu", hu);
	assert (s == "0042");
	
	unsigned x = 0x42;
	s = format("%x", x);
	assert (s == "42");
	s = format("%4x", x);
	assert (s == "  42");
	s = format("%04x", x);
	assert (s == "0042");

	unsigned o = 042;
	s = format("%o", o);
	assert (s == "42");
	s = format("%4o", o);
	assert (s == "  42");
	s = format("%04o", o);
	assert (s == "0042");

	unsigned u = 42;
	s = format("%u", u);
	assert (s == "42");
	s = format("%4u", u);
	assert (s == "  42");
	s = format("%04u", u);
	assert (s == "0042");
	
	long l = 42;
	s = format("%ld", l);
	assert (s == "42");
	s = format("%4ld", l);
	assert (s == "  42");
	s = format("%04ld", l);
	assert (s == "0042");

	unsigned long ul = 42;
	s = format("%lu", ul);
	assert (s == "42");
	s = format("%4lu", ul);
	assert (s == "  42");
	s = format("%04lu", ul);
	assert (s == "0042");
	
	unsigned long xl = 0x42;
	s = format("%lx", xl);
	assert (s == "42");
	s = format("%4lx", xl);
	assert (s == "  42");
	s = format("%04lx", xl);
	assert (s == "0042");
	
	Int64 i64 = 42;
	s = format("%Ld", i64);
	assert (s == "42");
	s = format("%4Ld", i64);
	assert (s == "  42");
	s = format("%04Ld", i64);
	assert (s == "0042");
	
	UInt64 ui64 = 42;
	s = format("%Lu", ui64);
	assert (s == "42");
	s = format("%4Lu", ui64);
	assert (s == "  42");
	s = format("%04Lu", ui64);
	assert (s == "0042");
	
	x = 0xaa;
	s = format("%x", x);
	assert (s == "aa");
	s = format("%X", x);
	assert (s == "AA");
	
	i = 42;
	s = format("%+d", i);
	assert (s == "+42");

	i = -42;
	s = format("%+d", i);
	assert (s == "-42");
	s = format("%+04d", i);
	assert (s == "-042");
	s = format("%d", i);
	assert (s == "-42");

	s = format("%d", i);
	assert (s == "-42");
	
	x = 0x42;
	s = format("%#x", x);
	assert (s == "0x42");

	s = format("%d", l);
	assert (s == "[ERRFMT]");
}


void FormatTest::testBool()
{
	bool b = true;
	std::string s = format("%b", b);
	assert (s == "1");

	b = false;
	s = format("%b", b);
	assert (s == "0");

	std::vector<Poco::Any> bv;
	bv.push_back(false);
	bv.push_back(true);
	bv.push_back(false);
	bv.push_back(true);
	bv.push_back(false);
	bv.push_back(true);
	bv.push_back(false);
	bv.push_back(true);
	bv.push_back(false);
	bv.push_back(true);

	s.clear();
	format(s, "%b%b%b%b%b%b%b%b%b%b", bv);
	assert (s == "0101010101");
}


void FormatTest::testAnyInt()
{
	char c = 42;
	std::string s(format("%?i", c));
	assert (s == "42");
	
	bool b = true;
	s = format("%?i", b);
	assert (s == "1");

	signed char sc = -42;
	s = format("%?i", sc);
	assert (s == "-42");
	
	unsigned char uc = 65;
	s = format("%?i", uc);
	assert (s == "65");
	
	short ss = -134;
	s = format("%?i", ss);
	assert (s == "-134");
	
	unsigned short us = 200;
	s = format("%?i", us);
	assert (s == "200");
	
	int i = -12345;
	s = format("%?i", i);
	assert (s == "-12345");
	
	unsigned ui = 12345;
	s = format("%?i", ui);
	assert (s == "12345");
	
	long l = -54321;
	s = format("%?i", l);
	assert (s == "-54321");
	
	unsigned long ul = 54321;
	s = format("%?i", ul);
	assert (s == "54321");
	
	Int64 i64 = -12345678;
	s = format("%?i", i64);
	assert (s == "-12345678");

	UInt64 ui64 = 12345678;
	s = format("%?i", ui64);
	assert (s == "12345678");
	
	ss = 0x42;
	s = format("%?x", ss);
	assert (s == "42");

	ss = 042;
	s = format("%?o", ss);
	assert (s == "42");
}


void FormatTest::testFloatFix()
{
	double d = 1.5;
	std::string s(format("%f", d));
	assert (s.find("1.50") == 0);

	s = format("%10f", d);
	assert (s.find(" 1.50") != std::string::npos);

	s = format("%6.2f", d);
	assert (s == "  1.50");
	s = format("%-6.2f", d);
	assert (s == "1.50  ");
	
	float f = 1.5;
	s = format("%hf", f);
	assert (s.find("1.50") == 0);

	s = format("%.0f", 1.0);
	assert (s == "1");
}


void FormatTest::testFloatSci()
{
	double d = 1.5;
	std::string s(format("%e", d));
	assert (s.find("1.50") == 0);
	assert (s.find("0e+0") != std::string::npos);

	s = format("%20e", d);
	assert (s.find(" 1.50") != std::string::npos);
	assert (s.find("0e+0") != std::string::npos);

	s = format("%10.2e", d);
	assert (s == " 1.50e+000" || s == "  1.50e+00");
	s = format("%-10.2e", d);
	assert (s == "1.50e+000 " || s == "1.50e+00  ");
	s = format("%-10.2E", d);
	assert (s == "1.50E+000 " || s == "1.50E+00  ");
}


void FormatTest::testString()
{
	std::string foo("foo");
	std::string s(format("%s", foo));
	assert (s == "foo");

	s = format("%5s", foo);
	assert (s == "  foo");

	s = format("%-5s", foo);
	assert (s == "foo  ");

	s = format("%s%%a", foo);
	assert (s == "foo%a");

	s = format("'%s%%''%s%%'", foo, foo);
	assert (s == "'foo%''foo%'");
}


void FormatTest::testMultiple()
{
	std::string s(format("aaa%dbbb%4dccc", 1, 2));
	assert (s == "aaa1bbb   2ccc");

	s = format("%%%d%%%d%%%d", 1, 2, 3);
	assert (s == "%1%2%3");
	
	s = format("%d%d%d%d", 1, 2, 3, 4);
	assert (s == "1234");

	s = format("%d%d%d%d%d", 1, 2, 3, 4, 5);
	assert (s == "12345");

	s = format("%d%d%d%d%d%d", 1, 2, 3, 4, 5, 6);
	assert (s == "123456");
}


void FormatTest::testIndex()
{
	std::string s(format("%[1]d%[0]d", 1, 2));
	assert(s == "21");

	s = format("%[5]d%[4]d%[3]d%[2]d%[1]d%[0]d", 1, 2, 3, 4, 5, 6);
	assert(s == "654321");

	s = format("%%%[1]d%%%[2]d%%%d", 1, 2, 3);
	assert(s == "%2%3%1");

	s = format("%%%d%%%d%%%[0]d", 1, 2);
	assert(s == "%1%2%1");
}


void FormatTest::setUp()
{
}


void FormatTest::tearDown()
{
}


CppUnit::Test* FormatTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("FormatTest");

	CppUnit_addTest(pSuite, FormatTest, testChar);
	CppUnit_addTest(pSuite, FormatTest, testBool);
	CppUnit_addTest(pSuite, FormatTest, testInt);
	CppUnit_addTest(pSuite, FormatTest, testAnyInt);
	CppUnit_addTest(pSuite, FormatTest, testFloatFix);
	CppUnit_addTest(pSuite, FormatTest, testFloatSci);
	CppUnit_addTest(pSuite, FormatTest, testString);
	CppUnit_addTest(pSuite, FormatTest, testMultiple);
	CppUnit_addTest(pSuite, FormatTest, testIndex);

	return pSuite;
}
