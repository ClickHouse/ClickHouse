//
// StringTokenizerTest.cpp
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//
#include <iostream>

#include "StringTokenizerTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/StringTokenizer.h"
#include "Poco/Exception.h"


using Poco::StringTokenizer;
using Poco::RangeException;
using Poco::NotFoundException;


StringTokenizerTest::StringTokenizerTest(const std::string& name): CppUnit::TestCase(name)
{
}


StringTokenizerTest::~StringTokenizerTest()
{
}


void StringTokenizerTest::testStringTokenizer()
{
	{
		StringTokenizer st("", "");
		assert (st.begin() == st.end());
	}
	{
		StringTokenizer st("", "", StringTokenizer::TOK_IGNORE_EMPTY);
		assert (st.begin() == st.end());
	}
	{
		StringTokenizer st("", "", StringTokenizer::TOK_TRIM);
		assert (st.begin() == st.end());
	}
	{
		StringTokenizer st("", "", StringTokenizer::TOK_TRIM | StringTokenizer::TOK_IGNORE_EMPTY);
		assert (st.begin() == st.end());
	}
	{
		StringTokenizer st("abc", "");
		StringTokenizer::Iterator it = st.begin();
		assert (st.find("abc") == 0);
		assert (it != st.end());
		assert (*it++ == "abc");
		assert (it == st.end());
	}
	{
		StringTokenizer st("abc ", "", StringTokenizer::TOK_TRIM);
		StringTokenizer::Iterator it = st.begin();
		assert (st.find("abc") == 0);
		assert (it != st.end());
		assert (*it++ == "abc");
		assert (it == st.end());
	}	
	{
		StringTokenizer st("  abc  ", "", StringTokenizer::TOK_TRIM);
		StringTokenizer::Iterator it = st.begin();
		assert (st.find("abc") == 0);
		assert (it != st.end());
		assert (*it++ == "abc");
		assert (it == st.end());
	}
	{
		StringTokenizer st("  abc", "", StringTokenizer::TOK_TRIM);
		StringTokenizer::Iterator it = st.begin();
		assert (st.find("abc") == 0);
		assert (it != st.end());
		assert (*it++ == "abc");
		assert (it == st.end());
	}
	{
		StringTokenizer st("abc", "b");
		StringTokenizer::Iterator it = st.begin();
		assert (st.find("a") == 0);
		assert (st.find("c") == 1);
		assert (it != st.end());
		assert (*it++ == "a");
		assert (it != st.end());
		assert (*it++ == "c");
		assert (it == st.end());
	}
	{
		StringTokenizer st("abc", "b", StringTokenizer::TOK_TRIM | StringTokenizer::TOK_IGNORE_EMPTY);
		StringTokenizer::Iterator it = st.begin();
		assert (st.find("a") == 0);
		assert (st.find("c") == 1);
		assert (it != st.end());
		assert (*it++ == "a");
		assert (it != st.end());
		assert (*it++ == "c");
		assert (it == st.end());
	}
	{
		StringTokenizer st("abc", "bc");
		StringTokenizer::Iterator it = st.begin();
		assert (st.find("a") == 0);
		assert (st.find("") == 1);
		assert (it != st.end());
		assert (*it++ == "a");
		assert (it != st.end());
		assert (*it++ == "");
		assert (it != st.end());
		assert (*it++ == "");
		assert (it == st.end());
	}
	{
		StringTokenizer st("abc", "bc", StringTokenizer::TOK_TRIM);
		StringTokenizer::Iterator it = st.begin();
		assert (st.find("a") == 0);
		assert (st.find("") == 1);
		assert (it != st.end());
		assert (*it++ == "a");
		assert (it != st.end());
		assert (*it++ == "");
		assert (it != st.end());
		assert (*it++ == "");
		assert (it == st.end());
	}
	{
		StringTokenizer st("abc", "bc", StringTokenizer::TOK_IGNORE_EMPTY);
		StringTokenizer::Iterator it = st.begin();
		assert (st.find("a") == 0);
		assert (it != st.end());
		assert (*it++ == "a");
		assert (it == st.end());
	}
	{
		StringTokenizer st("abc", "bc", StringTokenizer::TOK_TRIM | StringTokenizer::TOK_IGNORE_EMPTY);
		StringTokenizer::Iterator it = st.begin();
		assert (st.find("a") == 0);
		assert (it != st.end());
		assert (*it++ == "a");
		assert (it == st.end());
	}
	{
		StringTokenizer st("abc", "bc", StringTokenizer::TOK_TRIM | StringTokenizer::TOK_IGNORE_EMPTY);
		StringTokenizer::Iterator it = st.begin();
		assert (st.find("a") == 0);
		assert (it != st.end());
		assert (*it++ == "a");
		assert (it == st.end());
	}
	{
		StringTokenizer st("a a,c c", ",");
		StringTokenizer::Iterator it = st.begin();
		assert (st.find("a a") == 0);
		assert (st.find("c c") == 1);
		assert (it != st.end());
		assert (*it++ == "a a");
		assert (it != st.end());
		assert (*it++ == "c c");
		assert (it == st.end());
	}
	{
		StringTokenizer st("a a,c c", ",", StringTokenizer::TOK_TRIM | StringTokenizer::TOK_IGNORE_EMPTY);
		StringTokenizer::Iterator it = st.begin();
		assert (st.find("a a") == 0);
		assert (st.find("c c") == 1);
		assert (it != st.end());
		assert (*it++ == "a a");
		assert (it != st.end());
		assert (*it++ == "c c");
		assert (it == st.end());
	}
	{
		StringTokenizer st(" a a , , c c ", ",");
		StringTokenizer::Iterator it = st.begin();
		assert (st.find(" a a ") == 0);
		assert (st.find(" ") == 1);
		assert (st.find(" c c ") == 2);
		assert (it != st.end());
		assert (*it++ == " a a ");
		assert (it != st.end());
		assert (*it++ == " ");
		assert (it != st.end());
		assert (*it++ == " c c ");
		assert (it == st.end());
	}
	{
		StringTokenizer st(" a a , , c c ", ",", StringTokenizer::TOK_TRIM);
		StringTokenizer::Iterator it = st.begin();
		assert (st.find("a a") == 0);
		assert (st.find("") == 1);
		assert (st.find("c c") == 2);
		assert (it != st.end());
		assert (*it++ == "a a");
		assert (it != st.end());
		assert (*it++ == "");
		assert (it != st.end());
		assert (*it++ == "c c");
		assert (it == st.end());
	}
	{
		StringTokenizer st(" a a , , c c ", ",", StringTokenizer::TOK_TRIM | StringTokenizer::TOK_IGNORE_EMPTY);
		StringTokenizer::Iterator it = st.begin();
		assert (st.find("a a") == 0);
		assert (st.find("c c") == 1);
		assert (it != st.end());
		assert (*it++ == "a a");
		assert (it != st.end());
		assert (*it++ == "c c");
		assert (it == st.end());
	}
	{
		StringTokenizer st("abc,def,,ghi , jk,  l ", ",", StringTokenizer::TOK_TRIM | StringTokenizer::TOK_IGNORE_EMPTY);
		StringTokenizer::Iterator it = st.begin();
		assert (st.find("abc") == 0);
		assert (st.find("def") == 1);
		assert (st.find("ghi") == 2);
		assert (st.find("jk") == 3);
		assert (st.find("l") == 4);
		assert (it != st.end());
		assert (*it++ == "abc");
		assert (it != st.end());
		assert (*it++ == "def");
		assert (it != st.end());
		assert (*it++ == "ghi");
		assert (it != st.end());
		assert (*it++ == "jk");
		assert (it != st.end());
		assert (*it++ == "l");
		assert (it == st.end());
	}
	{
		StringTokenizer st("abc,def,,ghi // jk,  l ", ",/", StringTokenizer::TOK_TRIM | StringTokenizer::TOK_IGNORE_EMPTY);
		StringTokenizer::Iterator it = st.begin();
		assert (st.find("abc") == 0);
		assert (st.find("def") == 1);
		assert (st.find("ghi") == 2);
		assert (st.find("jk") == 3);
		assert (st.find("l") == 4);
		assert (it != st.end());
		assert (*it++ == "abc");
		assert (it != st.end());
		assert (*it++ == "def");
		assert (it != st.end());
		assert (*it++ == "ghi");
		assert (it != st.end());
		assert (*it++ == "jk");
		assert (it != st.end());
		assert (*it++ == "l");
		assert (it == st.end());
	}
	{
		StringTokenizer st("a/bc,def,,ghi // jk,  l ", ",/", StringTokenizer::TOK_TRIM | StringTokenizer::TOK_IGNORE_EMPTY);
		StringTokenizer::Iterator it = st.begin();
		assert (st.find("a") == 0);
		assert (st.find("bc") == 1);
		assert (st.find("def") == 2);
		assert (st.find("ghi") == 3);
		assert (st.find("jk") == 4);
		assert (st.find("l") == 5);
		assert (it != st.end());
		assert (*it++ == "a");
		assert (it != st.end());
		assert (*it++ == "bc");
		assert (it != st.end());
		assert (*it++ == "def");
		assert (it != st.end());
		assert (*it++ == "ghi");
		assert (it != st.end());
		assert (*it++ == "jk");
		assert (it != st.end());
		assert (*it++ == "l");
		assert (it == st.end());
	}
	{
		StringTokenizer st(",ab,cd,", ",");
		StringTokenizer::Iterator it = st.begin();
		assert (st.find("") == 0);
		assert (st.find("ab") == 1);
		assert (st.find("cd") == 2);
		assert (it != st.end());
		assert (*it++ == "");
		assert (it != st.end());
		assert (*it++ == "ab");
		assert (it != st.end());
		assert (*it++ == "cd");
		assert (it != st.end());
		assert (*it++ == "");
		assert (it == st.end());
	}
	{
		StringTokenizer st(",ab,cd,", ",", StringTokenizer::TOK_IGNORE_EMPTY);
		StringTokenizer::Iterator it = st.begin();
		assert (st.find("ab") == 0);
		assert (st.find("cd") == 1);
		assert (it != st.end());
		assert (*it++ == "ab");
		assert (it != st.end());
		assert (*it++ == "cd");
		assert (it == st.end());
	}
	{
		StringTokenizer st(" , ab , cd , ", ",", StringTokenizer::TOK_TRIM | StringTokenizer::TOK_IGNORE_EMPTY);
		StringTokenizer::Iterator it = st.begin();
		assert (st.find("ab") == 0);
		assert (st.find("cd") == 1);
		assert (it != st.end());
		assert (*it++ == "ab");
		assert (it != st.end());
		assert (*it++ == "cd");
		assert (it == st.end());
	}
	{
		StringTokenizer st("1 : 2 , : 3 ", ":,", StringTokenizer::TOK_TRIM | StringTokenizer::TOK_IGNORE_EMPTY);
		assert (st.count() == 3);
		assert (st[0] == "1");
		assert (st[1] == "2");
		assert (st[2] == "3");
		assert (st.find("1") == 0);
		assert (st.find("2") == 1);
		assert (st.find("3") == 2);
	}
	
	{
		Poco::StringTokenizer st(" 2- ","-", Poco::StringTokenizer::TOK_TRIM);
		assert (st.count() == 2);
		assert (st[0] == "2");
		assert (st[1] == "");
	}
}


void StringTokenizerTest::testFind()
{
	StringTokenizer st("0,1,2,3,3,2,1,0", ",", StringTokenizer::TOK_TRIM | StringTokenizer::TOK_IGNORE_EMPTY);
	
	assert (st.count() == 8);
	assert (2 == st.count("0"));
	assert (2 == st.count("1"));
	assert (2 == st.count("2"));
	assert (2 == st.count("3"));
	assert (0 == st.count("4"));
	assert (0 == st.count("5"));
	assert (0 == st.count("6"));
	assert (0 == st.count("7"));

	assert (st[0] == "0");
	assert (st[1] == "1");
	assert (st[2] == "2");
	assert (st[3] == "3");
	assert (st[4] == "3");
	assert (st[5] == "2");
	assert (st[6] == "1");
	assert (st[7] == "0");
	
	assert (st.has("0"));
	assert (st.has("1"));
	assert (st.has("2"));
	assert (st.has("3"));
	assert (!st.has("4"));
	assert (!st.has("5"));
	assert (!st.has("6"));
	assert (!st.has("7"));

	assert (st.find("0") == 0);
	assert (st.find("1") == 1);
	assert (st.find("2") == 2);
	assert (st.find("3") == 3);

	assert (st.find("0", 1) == 7);
	assert (st.find("1", 2) == 6);
	assert (st.find("2", 3) == 5);
	assert (st.find("3", 4) == 4);

	try
	{
		std::size_t POCO_UNUSED p = st.find("4");
		fail ("must fail");
	}
	catch (NotFoundException&) { }

	try
	{
		std::string POCO_UNUSED s = st[8];
		fail ("must fail");
	}
	catch (RangeException&) { }

	st[0] = "1";
	st[7] = "1";
	assert (st[0] == "1");
	assert (st[7] == "1");
	assert (0 == st.count("0"));
	assert (4 == st.count("1"));

	st.replace("2", "5");
	assert (0 == st.count("2"));
	assert (2 == st.count("5"));

	st.replace("3", "6", 4);
	assert (1 == st.count("3"));
	assert (1 == st.count("6"));
}


void StringTokenizerTest::setUp()
{
}


void StringTokenizerTest::tearDown()
{
}


CppUnit::Test* StringTokenizerTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("StringTokenizerTest");

	CppUnit_addTest(pSuite, StringTokenizerTest, testStringTokenizer);
	CppUnit_addTest(pSuite, StringTokenizerTest, testFind);

	return pSuite;
}
