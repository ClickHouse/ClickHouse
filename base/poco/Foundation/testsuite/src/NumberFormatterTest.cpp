//
// NumberFormatterTest.cpp
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "NumberFormatterTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/NumberFormatter.h"
#include <sstream>


using Poco::NumberFormatter;
using Poco::Int64;
using Poco::UInt64;


NumberFormatterTest::NumberFormatterTest(const std::string& name): CppUnit::TestCase(name)
{
}


NumberFormatterTest::~NumberFormatterTest()
{
}


void NumberFormatterTest::testFormat()
{
	assert (NumberFormatter::format(123) == "123");
	assert (NumberFormatter::format(-123) == "-123");
	assert (NumberFormatter::format(-123, 5) == " -123");

	assert (NumberFormatter::format((unsigned) 123) == "123");
	assert (NumberFormatter::format((unsigned) 123, 5) == "  123");
	assert (NumberFormatter::format0((unsigned) 123, 5) == "00123");
	
	assert (NumberFormatter::format((long) 123) == "123");
	assert (NumberFormatter::format((long) -123) == "-123");
	assert (NumberFormatter::format((long) -123, 5) == " -123");

	assert (NumberFormatter::format((unsigned long) 123) == "123");
	assert (NumberFormatter::format((unsigned long) 123, 5) == "  123");	

	assert (NumberFormatter::format(123) == "123");
	assert (NumberFormatter::format(-123) == "-123");
	assert (NumberFormatter::format(-123, 5) == " -123");

#if defined(POCO_HAVE_INT64)
	assert (NumberFormatter::format((Int64) 123) == "123");
	assert (NumberFormatter::format((Int64) -123) == "-123");
	assert (NumberFormatter::format((Int64) -123, 5) == " -123");

	assert (NumberFormatter::format((UInt64) 123) == "123");
	assert (NumberFormatter::format((UInt64) 123, 5) == "  123");	
#if defined(POCO_LONG_IS_64_BIT)
	assert (NumberFormatter::format((long long) 123) == "123");
	assert (NumberFormatter::format((long long) -123) == "-123");
	assert (NumberFormatter::format((long long) -123, 5) == " -123");

	assert (NumberFormatter::format((unsigned long long) 123) == "123");
	assert (NumberFormatter::format((unsigned long long) 123, 5) == "  123");
#endif
#endif

	if (sizeof(void*) == 4)
	{
		assert (NumberFormatter::format((void*) 0x12345678) == "12345678");
	}
	else
	{
		assert (NumberFormatter::format((void*) 0x12345678) == "0000000012345678");
	}
}


void NumberFormatterTest::testFormat0()
{
	assert (NumberFormatter::format0(123, 5) == "00123");
	assert (NumberFormatter::format0(-123, 5) == "-0123");
	assert (NumberFormatter::format0((long) 123, 5) == "00123");
	assert (NumberFormatter::format0((long) -123, 5) == "-0123");
	assert (NumberFormatter::format0((unsigned long) 123, 5) == "00123");

#if defined(POCO_HAVE_INT64)
	assert (NumberFormatter::format0((Int64) 123, 5) == "00123");
	assert (NumberFormatter::format0((Int64) -123, 5) == "-0123");
	assert (NumberFormatter::format0((UInt64) 123, 5) == "00123");
#if defined(POCO_LONG_IS_64_BIT)
	assert (NumberFormatter::format0((long long) 123, 5) == "00123");
	assert (NumberFormatter::format0((long long) -123, 5) == "-0123");
	assert (NumberFormatter::format0((unsigned long long) 123, 5) == "00123");
#endif
#endif
}


void NumberFormatterTest::testFormatBool()
{
	assert(NumberFormatter::format(true, NumberFormatter::FMT_TRUE_FALSE) == "true");
	assert(NumberFormatter::format(false, NumberFormatter::FMT_TRUE_FALSE) == "false");
	assert(NumberFormatter::format(true, NumberFormatter::FMT_YES_NO) == "yes");
	assert(NumberFormatter::format(false, NumberFormatter::FMT_YES_NO) == "no");
	assert(NumberFormatter::format(true, NumberFormatter::FMT_ON_OFF) == "on");
	assert(NumberFormatter::format(false, NumberFormatter::FMT_ON_OFF) == "off");
}


void NumberFormatterTest::testFormatHex()
{
	assert (NumberFormatter::formatHex(0x12) == "12");
	assert (NumberFormatter::formatHex(0xab) == "AB");
	assert (NumberFormatter::formatHex(0x12, 4) == "0012");
	assert (NumberFormatter::formatHex(0xab, 4) == "00AB");

	assert (NumberFormatter::formatHex((unsigned) 0x12) == "12");
	assert (NumberFormatter::formatHex((unsigned) 0xab) == "AB");
	assert (NumberFormatter::formatHex((unsigned) 0x12, 4) == "0012");
	assert (NumberFormatter::formatHex((unsigned) 0xab, 4) == "00AB");

	assert (NumberFormatter::formatHex((long) 0x12) == "12");
	assert (NumberFormatter::formatHex((long) 0xab) == "AB");
	assert (NumberFormatter::formatHex((long) 0x12, 4) == "0012");
	assert (NumberFormatter::formatHex((long) 0xab, 4) == "00AB");

	assert (NumberFormatter::formatHex((unsigned long) 0x12) == "12");
	assert (NumberFormatter::formatHex((unsigned long) 0xab) == "AB");
	assert (NumberFormatter::formatHex((unsigned long) 0x12, 4) == "0012");
	assert (NumberFormatter::formatHex((unsigned long) 0xab, 4) == "00AB");

#if defined(POCO_HAVE_INT64)
	assert (NumberFormatter::formatHex((Int64) 0x12) == "12");
	assert (NumberFormatter::formatHex((Int64) 0xab) == "AB");
	assert (NumberFormatter::formatHex((Int64) 0x12, 4) == "0012");
	assert (NumberFormatter::formatHex((Int64) 0xab, 4) == "00AB");

	assert (NumberFormatter::formatHex((UInt64) 0x12) == "12");
	assert (NumberFormatter::formatHex((UInt64) 0xab) == "AB");
	assert (NumberFormatter::formatHex((UInt64) 0x12, 4) == "0012");
	assert (NumberFormatter::formatHex((UInt64) 0xab, 4) == "00AB");
#if defined(POCO_LONG_IS_64_BIT)
	assert (NumberFormatter::formatHex((long long) 0x12) == "12");
	assert (NumberFormatter::formatHex((long long) 0xab) == "AB");
	assert (NumberFormatter::formatHex((long long) 0x12, 4) == "0012");
	assert (NumberFormatter::formatHex((long long) 0xab, 4) == "00AB");

	assert (NumberFormatter::formatHex((unsigned long long) 0x12) == "12");
	assert (NumberFormatter::formatHex((unsigned long long) 0xab) == "AB");
	assert (NumberFormatter::formatHex((unsigned long long) 0x12, 4) == "0012");
	assert (NumberFormatter::formatHex((unsigned long long) 0xab, 4) == "00AB");
#endif
#endif

	assert (NumberFormatter::formatHex(0x12, true) == "0x12");
	assert (NumberFormatter::formatHex(0xab, true) == "0xAB");
	assert (NumberFormatter::formatHex(0x12, 4, true) == "0x12");
	assert (NumberFormatter::formatHex(0xab, 4, true) == "0xAB");
	assert (NumberFormatter::formatHex(0x12, 6, true) == "0x0012");
	assert (NumberFormatter::formatHex(0xab, 6, true) == "0x00AB");

	assert (NumberFormatter::formatHex((unsigned) 0x12, true) == "0x12");
	assert (NumberFormatter::formatHex((unsigned) 0xab, true) == "0xAB");
	assert (NumberFormatter::formatHex((unsigned) 0x12, 4, true) == "0x12");
	assert (NumberFormatter::formatHex((unsigned) 0xab, 4, true) == "0xAB");
	assert (NumberFormatter::formatHex((unsigned) 0x12, 6, true) == "0x0012");
	assert (NumberFormatter::formatHex((unsigned) 0xab, 6, true) == "0x00AB");

	assert (NumberFormatter::formatHex((long) 0x12, true) == "0x12");
	assert (NumberFormatter::formatHex((long) 0xab, true) == "0xAB");
	assert (NumberFormatter::formatHex((long) 0x12, 4, true) == "0x12");
	assert (NumberFormatter::formatHex((long) 0xab, 4, true) == "0xAB");
	assert (NumberFormatter::formatHex((long) 0x12, 6, true) == "0x0012");
	assert (NumberFormatter::formatHex((long) 0xab, 6, true) == "0x00AB");

	assert (NumberFormatter::formatHex((unsigned long) 0x12, true) == "0x12");
	assert (NumberFormatter::formatHex((unsigned long) 0xab, true) == "0xAB");
	assert (NumberFormatter::formatHex((unsigned long) 0x12, 4, true) == "0x12");
	assert (NumberFormatter::formatHex((unsigned long) 0xab, 4, true) == "0xAB");
	assert (NumberFormatter::formatHex((unsigned long) 0x12, 6, true) == "0x0012");
	assert (NumberFormatter::formatHex((unsigned long) 0xab, 6, true) == "0x00AB");

#if defined(POCO_HAVE_INT64)
	assert (NumberFormatter::formatHex((Int64) 0x12, true) == "0x12");
	assert (NumberFormatter::formatHex((Int64) 0xab, true) == "0xAB");
	assert (NumberFormatter::formatHex((Int64) 0x12, 4, true) == "0x12");
	assert (NumberFormatter::formatHex((Int64) 0xab, 4, true) == "0xAB");
	assert (NumberFormatter::formatHex((Int64) 0x12, 6, true) == "0x0012");
	assert (NumberFormatter::formatHex((Int64) 0xab, 6, true) == "0x00AB");

	assert (NumberFormatter::formatHex((UInt64) 0x12, true) == "0x12");
	assert (NumberFormatter::formatHex((UInt64) 0xab, true) == "0xAB");
	assert (NumberFormatter::formatHex((UInt64) 0x12, 4, true) == "0x12");
	assert (NumberFormatter::formatHex((UInt64) 0xab, 4, true) == "0xAB");
	assert (NumberFormatter::formatHex((UInt64) 0x12, 6, true) == "0x0012");
	assert (NumberFormatter::formatHex((UInt64) 0xab, 6, true) == "0x00AB");
#if defined(POCO_LONG_IS_64_BIT)
	assert (NumberFormatter::formatHex((long long) 0x12, true) == "0x12");
	assert (NumberFormatter::formatHex((long long) 0xab, true) == "0xAB");
	assert (NumberFormatter::formatHex((long long) 0x12, 4, true) == "0x12");
	assert (NumberFormatter::formatHex((long long) 0xab, 4, true) == "0xAB");
	assert (NumberFormatter::formatHex((long long) 0x12, 6, true) == "0x0012");
	assert (NumberFormatter::formatHex((long long) 0xab, 6, true) == "0x00AB");

	assert (NumberFormatter::formatHex((unsigned long long) 0x12, true) == "0x12");
	assert (NumberFormatter::formatHex((unsigned long long) 0xab, true) == "0xAB");
	assert (NumberFormatter::formatHex((unsigned long long) 0x12, 4, true) == "0x12");
	assert (NumberFormatter::formatHex((unsigned long long) 0xab, 4, true) == "0xAB");
	assert (NumberFormatter::formatHex((unsigned long long) 0x12, 6, true) == "0x0012");
	assert (NumberFormatter::formatHex((unsigned long long) 0xab, 6, true) == "0x00AB");
#endif
#endif
}


void NumberFormatterTest::testFormatFloat()
{
	assert (NumberFormatter::format(1.0f) == "1");
	assert (NumberFormatter::format(1.23f) == "1.23");
	assert (NumberFormatter::format(-1.23f) == "-1.23");
	assert (NumberFormatter::format(0.1f) == "0.1");
	assert (NumberFormatter::format(-0.1f) == "-0.1");
	assert (NumberFormatter::format(1.23) == "1.23");
	assert (NumberFormatter::format(-1.23) == "-1.23");
	assert (NumberFormatter::format(1.0) == "1");
	assert (NumberFormatter::format(-1.0) == "-1");
	assert (NumberFormatter::format(0.1) == "0.1");
	assert (NumberFormatter::format(-0.1) == "-0.1");

	int decDigits = std::numeric_limits<double>::digits10;
	std::ostringstream ostr;
	ostr << "0." << std::string(decDigits - 1, '0') << '1';
	assert(NumberFormatter::format(1 / std::pow(10., decDigits)) == ostr.str());

	ostr.str("");
	ostr << "1e-" << decDigits + 1;
	std::string str (ostr.str());
	std::string str1 (NumberFormatter::format(1 / std::pow(10., decDigits + 1)));
	assert(NumberFormatter::format(1 / std::pow(10., decDigits + 1)) == ostr.str());

	assert(NumberFormatter::format(12.25) == "12.25");
	assert(NumberFormatter::format(12.25, 4) == "12.2500");
	assert(NumberFormatter::format(12.25, 8, 4) == " 12.2500");

	assert (NumberFormatter::format(12.45f, 2) == "12.45");

	assert(NumberFormatter::format(-12.25) == "-12.25");
	assert(NumberFormatter::format(-12.25, 4) == "-12.2500");
	assert(NumberFormatter::format(-12.25, 10, 4) == "  -12.2500");
	assert(NumberFormatter::format(-12.25, 10, 2) == "    -12.25");
	assert(NumberFormatter::format(-12.25, 10, 1) == "     -12.3");

	assert (NumberFormatter::format(50.0, 3) == "50.000");
	assert (NumberFormatter::format(50.0f, 3) == "50.000");
	assert (NumberFormatter::format(50.123, 3) == "50.123");
	assert (NumberFormatter::format(50.123f, 3) == "50.123");
	assert (NumberFormatter::format(50.123, 0) == "50");
	assert (NumberFormatter::format(50.123f, 0) == "50");
	assert (NumberFormatter::format(50.546, 0) == "51");
	assert (NumberFormatter::format(50.546f, 0) == "51");
	assert (NumberFormatter::format(50.546f, 2) == "50.55");
}


void NumberFormatterTest::testAppend()
{
	std::string s;
	NumberFormatter::append(s, 123);
	assert (s == "123");
	s.erase();
	NumberFormatter::append(s, 123, 4);
	assert (s == " 123");
	s.erase();
	NumberFormatter::append0(s, 123, 5);
	assert (s == "00123");
	s.erase();
	NumberFormatter::appendHex(s, 0xDEAD);
	assert (s == "DEAD");
	s.erase();
	NumberFormatter::appendHex(s, 0xDEAD, 6);
	assert (s == "00DEAD");
	s.erase();
	NumberFormatter::append(s, 123u);
	assert (s == "123");
	s.erase();
	NumberFormatter::append(s, 123u, 4);
	assert (s == " 123");
	s.erase();
	NumberFormatter::append0(s, 123u, 5);
	assert (s == "00123");

	
	s.erase();
	NumberFormatter::append(s, 123.4);
	assert (s == "123.4");
	s.erase();
	NumberFormatter::append(s, 123.4567, 2);
	assert (s == "123.46");
	s.erase();
	NumberFormatter::append(s, 123.4567, 10, 5);
	assert (s == " 123.45670");
	s.erase();
	NumberFormatter::append(s, 123., 2);
	assert (s == "123.00");
	s.erase();
	NumberFormatter::append(s, static_cast<double>(1234567), 2);
	assert (s == "1234567.00");
	s.erase();
	NumberFormatter::append(s, 1234567.0, 10, 1);
	assert (s == " 1234567.0");
}


void NumberFormatterTest::setUp()
{
}


void NumberFormatterTest::tearDown()
{
}


CppUnit::Test* NumberFormatterTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("NumberFormatterTest");

	CppUnit_addTest(pSuite, NumberFormatterTest, testFormat);
	CppUnit_addTest(pSuite, NumberFormatterTest, testFormat0);
	CppUnit_addTest(pSuite, NumberFormatterTest, testFormatBool);
	CppUnit_addTest(pSuite, NumberFormatterTest, testFormatHex);
	CppUnit_addTest(pSuite, NumberFormatterTest, testFormatFloat);
	CppUnit_addTest(pSuite, NumberFormatterTest, testAppend);

	return pSuite;
}
