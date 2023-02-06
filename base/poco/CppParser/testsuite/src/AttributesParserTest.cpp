//
// AttributesParserTest.cpp
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "AttributesParserTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/CppParser/Attributes.h"
#include "Poco/CppParser/AttributesParser.h"
#include <sstream>


using Poco::CppParser::Attributes;
using Poco::CppParser::AttributesParser;


AttributesParserTest::AttributesParserTest(const std::string& name): CppUnit::TestCase(name)
{
}


AttributesParserTest::~AttributesParserTest()
{
}


void AttributesParserTest::testParser1()
{
	Attributes attrs;
	std::istringstream istr("");
	AttributesParser parser(attrs, istr);
	parser.parse();
	assert (attrs.begin() == attrs.end());
}


void AttributesParserTest::testParser2()
{
	Attributes attrs;
	std::istringstream istr("name=value");
	AttributesParser parser(attrs, istr);
	parser.parse();
	assert (attrs.getString("name") == "value");
}


void AttributesParserTest::testParser3()
{
	Attributes attrs;
	std::istringstream istr("name=value, name2=100");
	AttributesParser parser(attrs, istr);
	parser.parse();
	assert (attrs.getString("name") == "value");
	assert (attrs.getInt("name2") == 100);
}


void AttributesParserTest::testParser4()
{
	Attributes attrs;
	std::istringstream istr("name=value, name2=100, name3");
	AttributesParser parser(attrs, istr);
	parser.parse();
	assert (attrs.getString("name") == "value");
	assert (attrs.getInt("name2") == 100);
	assert (attrs.getBool("name3"));
}


void AttributesParserTest::testParser5()
{
	Attributes attrs;
	std::istringstream istr("name.a=value, name.b=100, name.c");
	AttributesParser parser(attrs, istr);
	parser.parse();
	assert (attrs.getString("name.a") == "value");
	assert (attrs.getInt("name.b") == 100);
	assert (attrs.getBool("name.c"));
}


void AttributesParserTest::testParser6()
{
	Attributes attrs;
	std::istringstream istr("name = {a=value, b=100, c}");
	AttributesParser parser(attrs, istr);
	parser.parse();
	assert (attrs.getString("name.a") == "value");
	assert (attrs.getInt("name.b") == 100);
	assert (attrs.getBool("name.c"));
}


void AttributesParserTest::testParser7()
{
	Attributes attrs;
	std::istringstream istr("name = {a=value, b=100, c}, name2=\"foo\"");
	AttributesParser parser(attrs, istr);
	parser.parse();
	assert (attrs.getString("name.a") == "value");
	assert (attrs.getInt("name.b") == 100);
	assert (attrs.getBool("name.c"));
	assert (attrs.getString("name2") == "foo");
}


void AttributesParserTest::setUp()
{
}


void AttributesParserTest::tearDown()
{
}


CppUnit::Test* AttributesParserTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("AttributesParserTest");

	CppUnit_addTest(pSuite, AttributesParserTest, testParser1);
	CppUnit_addTest(pSuite, AttributesParserTest, testParser2);
	CppUnit_addTest(pSuite, AttributesParserTest, testParser3);
	CppUnit_addTest(pSuite, AttributesParserTest, testParser4);
	CppUnit_addTest(pSuite, AttributesParserTest, testParser5);
	CppUnit_addTest(pSuite, AttributesParserTest, testParser6);
	CppUnit_addTest(pSuite, AttributesParserTest, testParser7);

	return pSuite;
}
