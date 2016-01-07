//
// NumberParserTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/NumberParserTest.h#1 $
//
// Definition of the NumberParserTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NumberParserTest_INCLUDED
#define NumberParserTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"
#include "Poco/NumberParser.h"
#include "Poco/NumberFormatter.h"
#undef max
#undef min
#include <limits>

class NumberParserTest: public CppUnit::TestCase
{
public:
	NumberParserTest(const std::string& name);
	~NumberParserTest();

	void testParse();
	void testLimits();
	void testParseError();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:

	template <class T> bool testUpperLimit()
	{
		T n = std::numeric_limits<T>::max();
		std::string s = Poco::NumberFormatter::format(n);
		if (std::numeric_limits<T>::is_signed)
			return Poco::NumberParser::parse(s) == n;
		else
			return Poco::NumberParser::parseUnsigned(s) == n;
	}

	template <class T> bool testLowerLimit()
	{
		T n = std::numeric_limits<T>::min();
		std::string s = Poco::NumberFormatter::format(n);
		return Poco::NumberParser::parse(s) == n;
	}

#if defined(POCO_HAVE_INT64)
	template <class T> bool testUpperLimit64()
	{
		T n = std::numeric_limits<T>::max();
		std::string s = Poco::NumberFormatter::format(n);
		if (std::numeric_limits<T>::is_signed)
			return Poco::NumberParser::parse64(s) == n;
		else
			return Poco::NumberParser::parseUnsigned64(s) == n;
	}

	template <class T> bool testLowerLimit64()
	{
		T n = std::numeric_limits<T>::min();
		std::string s = Poco::NumberFormatter::format(n);
		return Poco::NumberParser::parse64(s) == n;
	}
#endif
};


#endif // NumberParserTest_INCLUDED
