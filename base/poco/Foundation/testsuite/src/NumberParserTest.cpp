//
// NumberParserTest.cpp
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "NumberParserTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Exception.h"
#include "Poco/Types.h"
#include "Poco/Format.h"
#include "Poco/NumericString.h"
#include "Poco/MemoryStream.h"
#include "Poco/Stopwatch.h"
#include <iostream>
#include <iomanip>
#include <cstdio>


using Poco::NumberParser;
using Poco::NumberFormatter;
using Poco::SyntaxException;
using Poco::Int8;
using Poco::UInt8;
using Poco::Int16;
using Poco::UInt16;
using Poco::Int32;
using Poco::UInt32;
#if defined(POCO_HAVE_INT64)
using Poco::Int64;
using Poco::UInt64;
#endif
using Poco::format;
using Poco::decimalSeparator;
using Poco::thousandSeparator;


NumberParserTest::NumberParserTest(const std::string& name): CppUnit::TestCase(name)
{
}


NumberParserTest::~NumberParserTest()
{
}


void NumberParserTest::testParse()
{
	std::string sep(".,");

	for (int i = 0; i < 2; ++i)
	{
		char ts = sep[i];

		assert(NumberParser::parse("123") == 123);
		assert(NumberParser::parse(format("123%c456", ts), ts) == 123456);
		assert(NumberParser::parse(format("1%c234%c567", ts, ts), ts) == 1234567);
	}

	assert(NumberParser::parse("+123") == 123);
	assert(NumberParser::parse("-123") == -123);
	assert(NumberParser::parse("0") == 0);
	assert(NumberParser::parse("000") == 0);
	assert(NumberParser::parse("0123") == 123);
	assert(NumberParser::parse("+0123") == 123);
	assert(NumberParser::parse("-0123") == -123);
	assert(NumberParser::parseUnsigned("123") == 123);
	assert(NumberParser::parseHex("12AB") == 0x12ab);
	assert(NumberParser::parseHex("0x12AB") == 0x12ab);
	assert(NumberParser::parseHex("0X12AB") == 0x12ab);
	assert(NumberParser::parseHex("00") == 0);
	assert(NumberParser::parseOct("123") == 0123);
	assert(NumberParser::parseOct("0123") == 0123);
	assert(NumberParser::parseOct("0") == 0);
	assert(NumberParser::parseOct("000") == 0);

	assert(NumberParser::parseBool("0") == false);
	assert(NumberParser::parseBool("FALSE") == false);
	assert(NumberParser::parseBool("no") == false);
	assert(NumberParser::parseBool("1") == true);
	assert(NumberParser::parseBool("True") == true);
	assert(NumberParser::parseBool("YeS") == true);

#if defined(POCO_HAVE_INT64)
	assert(NumberParser::parse64("123") == 123);
	assert(NumberParser::parse64("-123") == -123);
	assert(NumberParser::parse64("0123") == 123);
	assert(NumberParser::parse64("-0123") == -123);
	assert(NumberParser::parseUnsigned64("123") == 123);
	assert(NumberParser::parseHex64("12AB") == 0x12ab);
	assert(NumberParser::parseOct64("123") == 0123);
	assert(NumberParser::parseOct64("0123") == 0123);
#endif

#ifndef POCO_NO_FPENVIRONMENT
	for (int i = 0; i < 2; ++i)
	{
		char ts = sep[i];
		for (int j = 0; j < 2; ++j)
		{
			char dp = sep[j];
			if (ts == dp) continue;

			assertEqualDelta(1.0, NumberParser::parseFloat(format("1", dp), dp, ts), 0.01);
			assertEqualDelta(0.0, NumberParser::parseFloat(format("0%c0", dp), dp, ts), 0.01);
			assertEqualDelta(0., NumberParser::parseFloat(format("0%c0", dp), dp, ts), 0.01);
			assertEqualDelta(.0, NumberParser::parseFloat(format("0%c0", dp), dp, ts), 0.01);
			assertEqualDelta(12.34, NumberParser::parseFloat(format("12%c34", dp), dp, ts), 0.01);
			assertEqualDelta(12.34, NumberParser::parseFloat(format("12%c34f", dp), dp, ts), 0.01);
			assertEqualDelta(12.34, NumberParser::parseFloat(format("12%c34", dp), dp, ts), 0.01);
			assertEqualDelta(-12.34, NumberParser::parseFloat(format("-12%c34", dp), dp, ts), 0.01);
			assertEqualDelta(.34, NumberParser::parseFloat(format("%c34", dp), dp, ts), 0.01);
			assertEqualDelta(-.34, NumberParser::parseFloat(format("-%c34", dp), dp, ts), 0.01);
			assertEqualDelta(12., NumberParser::parseFloat(format("12%c", dp), dp, ts), 0.01);
			assertEqualDelta(-12., NumberParser::parseFloat(format("-12%c", dp), dp, ts), 0.01);
			assertEqualDelta(12, NumberParser::parseFloat("12", dp, ts), 0.01);
			assertEqualDelta(-12, NumberParser::parseFloat("-12", dp, ts), 0.01);
			assertEqualDelta(12.34, NumberParser::parseFloat(format("12%c3456789012345678901234567890", dp), dp, ts), 0.01);

			assertEqualDelta(1234.3456789, NumberParser::parseFloat(format("1%c234%c3456789012345678901234567890", ts, dp), dp, ts), 0.00000001);
			assertEqualDelta(12345.3456789, NumberParser::parseFloat(format("12%c345%c3456789012345678901234567890", ts, dp), dp, ts), 0.00000001);
			assertEqualDelta(123456.3456789, NumberParser::parseFloat(format("123%c456%c3456789012345678901234567890", ts, dp), dp, ts), 0.00000001);
			assertEqualDelta(1234567.3456789, NumberParser::parseFloat(format("1%c234%c567%c3456789012345678901234567890", ts, ts, dp), dp, ts), 0.00000001);
			assertEqualDelta(12345678.3456789, NumberParser::parseFloat(format("12%c345%c678%c3456789012345678901234567890", ts, ts, dp), dp, ts), 0.00000001);
			assertEqualDelta(123456789.3456789, NumberParser::parseFloat(format("123%c456%c789%c3456789012345678901234567890", ts, ts, dp), dp, ts), 0.00000001);

			if ((std::numeric_limits<double>::max() / 10) < 1.23456e10)
				fail ("test value larger than max value for this platform");
			else
			{
				double d = 1.234e100;
				assertEqualDelta(d, NumberParser::parseFloat(format("1%c234e100", dp), dp, ts), 0.01);
				assertEqualDelta(d, NumberParser::parseFloat(format("1%c234E+100", dp), dp, ts), 0.01);
		
				d = 1.234e-100;
				assertEqualDelta(d, NumberParser::parseFloat(format("1%c234E-100", dp), dp, ts), 0.01);
		
				d = -1.234e100;
				assertEqualDelta(d, NumberParser::parseFloat(format("-1%c234e+100", dp), dp, ts), 0.01);
				assertEqualDelta(d, NumberParser::parseFloat(format("-1%c234E100", dp), dp, ts), 0.01);
		
				d = 1234.234e-100;
				assertEqualDelta(d, NumberParser::parseFloat(format("1%c234%c234e-100", ts, dp), dp, ts), 0.01);
				d = 12345.234e-100;
				assertEqualDelta(d, NumberParser::parseFloat(format("12%c345%c234e-100", ts, dp), dp, ts), 0.01);
				d = 123456.234e-100;
				assertEqualDelta(d, NumberParser::parseFloat(format("123%c456%c234e-100", ts, dp), dp, ts), 0.01);

				d = -1234.234e-100;
				assertEqualDelta(d, NumberParser::parseFloat(format("-1%c234%c234e-100", ts, dp), dp, ts), 0.01);
				d = -12345.234e-100;
				assertEqualDelta(d, NumberParser::parseFloat(format("-12%c345%c234e-100", ts, dp), dp, ts), 0.01);
				d = -123456.234e-100;
				assertEqualDelta(d, NumberParser::parseFloat(format("-123%c456%c234e-100", ts, dp), dp, ts), 0.01);
			}

			double d = 12.34e-10;
			assertEqualDelta(d, NumberParser::parseFloat(format("12%c34e-10", dp), dp, ts), 0.01);
			assertEqualDelta(-12.34, NumberParser::parseFloat(format("-12%c34", dp), dp, ts), 0.01);
	
			assertEqualDelta(12.34, NumberParser::parseFloat(format("12%c34", dp), dp, ts), 0.01);
		}
	}
#endif // POCO_NO_FPENVIRONMENT
}


void NumberParserTest::testLimits()
{
	assert(testUpperLimit<Int8>());
	assert(testLowerLimit<Int8>());
	assert(testUpperLimit<UInt8>());
	assert(testUpperLimit<Int16>());
	assert(testLowerLimit<Int16>());
	assert(testUpperLimit<UInt16>());
	assert(testUpperLimit<Int32>());
	assert(testLowerLimit<Int32>());
	assert(testUpperLimit<UInt32>());

#if defined(POCO_HAVE_INT64)
	assert(testUpperLimit64<Int64>());
	assert(testLowerLimit64<Int64>());
	assert(testUpperLimit64<UInt64>());
#endif
}


void NumberParserTest::testParseError()
{
	char dp = decimalSeparator();
	if (dp == 0) dp = '.';
	char ts = thousandSeparator();
	if (ts == 0) ts = ',';
	assert (dp != ts);

	try
	{
		NumberParser::parse("");
		NumberParser::parseBool("");
		failmsg("must throw SyntaxException");
	} catch (SyntaxException&) { }

	try
	{
		NumberParser::parse(" ");
		NumberParser::parseBool("");
		failmsg("must throw SyntaxException");
	} catch (SyntaxException&) { }

	try
	{
		NumberParser::parse(" 123");
		NumberParser::parseBool("");
		failmsg("must throw SyntaxException");
	} catch (SyntaxException&) { }

	try
	{
		NumberParser::parse("1 1");
		NumberParser::parseBool("");
		failmsg("must throw SyntaxException");
	} catch (SyntaxException&) { }

	try
	{
		NumberParser::parse("asd");
		NumberParser::parseBool("asd");
		failmsg("must throw SyntaxException");
	} catch (SyntaxException&) { }

	try
	{
		NumberParser::parseUnsigned("a123");
		failmsg("must throw SyntaxException");
	} catch (SyntaxException&) { }

	try
	{
		NumberParser::parseUnsigned("-123");
		failmsg("must throw SyntaxException");
	} catch (SyntaxException&) { }

	try
	{
		NumberParser::parseHex("z23");
		failmsg("must throw SyntaxException");
	} catch (SyntaxException&) { }

	try
	{
		NumberParser::parseHex("23z");
		failmsg("must throw SyntaxException");
	} catch (SyntaxException&) { }
	
#if defined(POCO_HAVE_INT64)

	try
	{
		NumberParser::parse64("asd");
		failmsg("must throw SyntaxException");
	} catch (SyntaxException&) { }

	try
	{
		NumberParser::parseUnsigned64("");
		failmsg("must throw SyntaxException");
	} catch (SyntaxException&) { }

	try
	{
		NumberParser::parseHex64("zaz");
		failmsg("must throw SyntaxException");
	} catch (SyntaxException&) { }

	try
	{
		NumberParser::parseHex64("12345z");
		failmsg("must throw SyntaxException");
	} catch (SyntaxException&) { }
	
	try
	{
		NumberParser::parseHex64(format("123%c45", ts));
		failmsg("must throw SyntaxException");
	} catch (SyntaxException&) { }

#endif // POCO_HAVE_INT64

#ifndef POCO_NO_FPENVIRONMENT
	try
	{
		NumberParser::parseFloat(format("a12%c3", dp));
		failmsg("must throw SyntaxException");
	} catch (SyntaxException&) { }

#endif // POCO_NO_FPENVIRONMENT
}


void NumberParserTest::setUp()
{
}


void NumberParserTest::tearDown()
{
}


CppUnit::Test* NumberParserTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("NumberParserTest");

	CppUnit_addTest(pSuite, NumberParserTest, testParse);
	CppUnit_addTest(pSuite, NumberParserTest, testLimits);
	CppUnit_addTest(pSuite, NumberParserTest, testParseError);

	return pSuite;
}
