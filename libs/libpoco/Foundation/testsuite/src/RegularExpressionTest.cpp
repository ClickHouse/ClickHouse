//
// RegularExpressionTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/RegularExpressionTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "RegularExpressionTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/RegularExpression.h"
#include "Poco/Exception.h"


using Poco::RegularExpression;
using Poco::RegularExpressionException;


RegularExpressionTest::RegularExpressionTest(const std::string& name): CppUnit::TestCase(name)
{
}


RegularExpressionTest::~RegularExpressionTest()
{
}


void RegularExpressionTest::testIndex()
{
	RegularExpression re("[0-9]+");
	RegularExpression::Match match;
	assert (re.match("", 0, match) == 0);
	assert (re.match("123", 3, match) == 0);
}


void RegularExpressionTest::testMatch1()
{
	RegularExpression re("[0-9]+");
	assert (re.match("123"));
	assert (!re.match("123cd"));
	assert (!re.match("abcde"));
	assert (re.match("ab123", 2));
}


void RegularExpressionTest::testMatch2()
{
	RegularExpression re("[0-9]+");
	RegularExpression::Match match;
	assert (re.match("123", 0, match) == 1);
	assert (match.offset == 0);
	assert (match.length == 3);

	assert (re.match("abc123def", 0, match) == 1);
	assert (match.offset == 3);
	assert (match.length == 3);

	assert (re.match("abcdef", 0, match) == 0);
	assert (match.offset == std::string::npos);
	assert (match.length == 0);

	assert (re.match("abc123def", 3, match) == 1);
	assert (match.offset == 3);
	assert (match.length == 3);
}


void RegularExpressionTest::testMatch3()
{
	RegularExpression re("[0-9]+");
	RegularExpression::MatchVec match;
	assert (re.match("123", 0, match) == 1);
	assert (match.size() == 1);
	assert (match[0].offset == 0);
	assert (match[0].length == 3);

	assert (re.match("abc123def", 0, match) == 1);
	assert (match.size() == 1);
	assert (match[0].offset == 3);
	assert (match[0].length == 3);

	assert (re.match("abcdef", 0, match) == 0);
	assert (match.size() == 0);

	assert (re.match("abc123def", 3, match) == 1);
	assert (match.size() == 1);
	assert (match[0].offset == 3);
	assert (match[0].length == 3);
}


void RegularExpressionTest::testMatch4()
{
	RegularExpression re("([0-9]+) ([0-9]+)");
	RegularExpression::MatchVec matches;
	assert (re.match("123 456", 0, matches) == 3);
	assert (matches.size() == 3);
	assert (matches[0].offset == 0);
	assert (matches[0].length == 7);
	assert (matches[1].offset == 0);
	assert (matches[1].length == 3);
	assert (matches[2].offset == 4);
	assert (matches[2].length == 3);

	assert (re.match("abc123 456def", 0, matches) == 3);
	assert (matches.size() == 3);
	assert (matches[0].offset == 3);
	assert (matches[0].length == 7);
	assert (matches[1].offset == 3);
	assert (matches[1].length == 3);
	assert (matches[2].offset == 7);
	assert (matches[2].length == 3);
}


void RegularExpressionTest::testMatch5()
{
	std::string digits = "0123";
	assert (RegularExpression::match(digits, "[0-9]+"));
	std::string alphas = "abcd";
	assert (!RegularExpression::match(alphas, "[0-9]+"));
}


void RegularExpressionTest::testMatch6()
{
	RegularExpression expr("^([a-z]*)?$");
	assert (expr.match("", 0, 0));
	assert (expr.match("abcde", 0, 0));
	assert (!expr.match("123", 0, 0));
}


void RegularExpressionTest::testExtract()
{
	RegularExpression re("[0-9]+");
	std::string str;
	assert (re.extract("123", str) == 1);
	assert (str == "123");

	assert (re.extract("abc123def", 0, str) == 1);
	assert (str == "123");

	assert (re.extract("abcdef", 0, str) == 0);
	assert (str == "");

	assert (re.extract("abc123def", 3, str) == 1);
	assert (str == "123");
}


void RegularExpressionTest::testSplit1()
{
	RegularExpression re("[0-9]+");
	std::vector<std::string> strings;
	assert (re.split("123", 0, strings) == 1);
	assert (strings.size() == 1);
	assert (strings[0] == "123");

	assert (re.split("abc123def", 0, strings) == 1);
	assert (strings.size() == 1);
	assert (strings[0] == "123");

	assert (re.split("abcdef", 0, strings) == 0);
	assert (strings.empty());

	assert (re.split("abc123def", 3, strings) == 1);
	assert (strings.size() == 1);
	assert (strings[0] == "123");
}


void RegularExpressionTest::testSplit2()
{
	RegularExpression re("([0-9]+) ([0-9]+)");
	std::vector<std::string> strings;
	assert (re.split("123 456", 0, strings) == 3);
	assert (strings.size() == 3);
	assert (strings[0] == "123 456");
	assert (strings[1] == "123");
	assert (strings[2] == "456");

	assert (re.split("abc123 456def", 0, strings) == 3);
	assert (strings.size() == 3);
	assert (strings[0] == "123 456");
	assert (strings[1] == "123");
	assert (strings[2] == "456");
}


void RegularExpressionTest::testSubst1()
{
	RegularExpression re("[0-9]+");
	std::string s = "123";
	assert (re.subst(s, "ABC") == 1);
	assert (s == "ABC");
	assert (re.subst(s, "123") == 0);

	s = "123";
	assert (re.subst(s, "AB$0CD") == 1);
	assert (s == "AB123CD");

	s = "123";
	assert (re.subst(s, "AB$1CD") == 1);
	assert (s == "ABCD");

	s = "123";
	assert (re.subst(s, "AB$2CD") == 1);
	assert (s == "ABCD");

	s = "123";
	assert (re.subst(s, "AB$$CD") == 1);
	assert (s == "AB$$CD");

	s = "123";
	assert (re.subst(s, "AB$0CD", RegularExpression::RE_NO_VARS) == 1);
	assert (s == "AB$0CD");
}


void RegularExpressionTest::testSubst2()
{
	RegularExpression re("([0-9]+) ([0-9]+)");
	std::string s = "123 456";
	assert (re.subst(s, "$2-$1") == 1);
	assert (s == "456-123");
}


void RegularExpressionTest::testSubst3()
{
	RegularExpression re("[0-9]+");
	std::string s = "123 456 789";
	assert (re.subst(s, "n", RegularExpression::RE_GLOBAL) == 3);
	assert (s == "n n n");
}


void RegularExpressionTest::testSubst4()
{
	RegularExpression re("[0-9]+");
	std::string s = "ABC 123 456 789 DEF";
	assert (re.subst(s, "n", RegularExpression::RE_GLOBAL) == 3);
	assert (s == "ABC n n n DEF");
}


void RegularExpressionTest::testError()
{
	try
	{
		RegularExpression re("(0-9]");
		failmsg("bad regexp - must throw exception");
	}
	catch (RegularExpressionException&)
	{
	}
}


void RegularExpressionTest::setUp()
{
}


void RegularExpressionTest::tearDown()
{
}


CppUnit::Test* RegularExpressionTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("RegularExpressionTest");

	CppUnit_addTest(pSuite, RegularExpressionTest, testIndex);
	CppUnit_addTest(pSuite, RegularExpressionTest, testMatch1);
	CppUnit_addTest(pSuite, RegularExpressionTest, testMatch2);
	CppUnit_addTest(pSuite, RegularExpressionTest, testMatch3);
	CppUnit_addTest(pSuite, RegularExpressionTest, testMatch4);
	CppUnit_addTest(pSuite, RegularExpressionTest, testMatch5);
	CppUnit_addTest(pSuite, RegularExpressionTest, testMatch6);
	CppUnit_addTest(pSuite, RegularExpressionTest, testExtract);
	CppUnit_addTest(pSuite, RegularExpressionTest, testSplit1);
	CppUnit_addTest(pSuite, RegularExpressionTest, testSplit2);
	CppUnit_addTest(pSuite, RegularExpressionTest, testSubst1);
	CppUnit_addTest(pSuite, RegularExpressionTest, testSubst2);
	CppUnit_addTest(pSuite, RegularExpressionTest, testSubst3);
	CppUnit_addTest(pSuite, RegularExpressionTest, testSubst4);
	CppUnit_addTest(pSuite, RegularExpressionTest, testError);

	return pSuite;
}
