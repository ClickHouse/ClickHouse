//
// MediaTypeTest.cpp
//
// $Id: //poco/1.4/Net/testsuite/src/MediaTypeTest.cpp#2 $
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "MediaTypeTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Net/MediaType.h"


using Poco::Net::MediaType;


MediaTypeTest::MediaTypeTest(const std::string& name): CppUnit::TestCase(name)
{
}


MediaTypeTest::~MediaTypeTest()
{
}


void MediaTypeTest::testParse()
{
	MediaType mt1("text/plain");
	assert (mt1.getType() == "text");
	assert (mt1.getSubType() == "plain");
	assert (mt1.parameters().empty());
	
	MediaType mt2("text/xml;charset=us-ascii");
	assert (mt2.getType() == "text");
	assert (mt2.getSubType() == "xml");
	assert (!mt2.parameters().empty());
	assert (mt2.getParameter("charset") == "us-ascii");
	
	MediaType mt3("application/test; param1=value1; param2=\"value 2\"");
	assert (mt3.getType() == "application");
	assert (mt3.getSubType() == "test");
	assert (!mt3.parameters().empty());
	assert (mt3.getParameter("param1") == "value1");
	assert (mt3.getParameter("PARAM2") == "value 2");
}


void MediaTypeTest::testToString()
{
	MediaType mt1("text", "plain");
	assert (mt1.toString() == "text/plain");
	
	mt1.setParameter("charset", "iso-8859-1");
	assert (mt1.toString() == "text/plain; charset=iso-8859-1");
	
	MediaType mt2("application", "test");
	mt2.setParameter("param1", "value1");
	mt2.setParameter("param2", "value 2");
	assert (mt2.toString() == "application/test; param1=value1; param2=\"value 2\"");
}


void MediaTypeTest::testMatch()
{
	MediaType mt1("Text/Plain");
	MediaType mt2("text/plain");
	MediaType mt3("text/xml");
	assert (mt1.matches(mt2));
	assert (!mt1.matches(mt3));
	assert (mt1.matches("text"));
	assert (mt2.matches("text"));
	assert (mt3.matches("text"));
}


void MediaTypeTest::testMatchRange()
{
	MediaType mt1("Text/Plain");
	MediaType mt2("text/plain");
	MediaType mt3("text/xml");
	MediaType mt4("image/jpg");
	MediaType mt5("text/*");
	MediaType mt6("*/*");
	assert (mt1.matchesRange(mt5));
	assert (mt2.matchesRange(mt5));
	assert (mt3.matchesRange(mt5));
	assert (!mt4.matchesRange(mt5));
	assert (mt1.matchesRange(mt6));
	assert (mt2.matchesRange(mt6));
	assert (mt3.matchesRange(mt6));
	assert (mt4.matchesRange(mt6));
	
	assert (mt5.matchesRange(mt1));
	assert (mt5.matchesRange(mt2));
	assert (mt5.matchesRange(mt3));
	assert (!mt5.matchesRange(mt4));
	
	assert (mt1.matchesRange("text", "*"));
	assert (mt2.matchesRange("text", "*"));
	assert (mt3.matchesRange("text", "*"));
	assert (!mt4.matchesRange("text", "*"));
	
	assert (mt1.matchesRange("*"));
	assert (mt4.matchesRange("*"));
}


void MediaTypeTest::setUp()
{
}


void MediaTypeTest::tearDown()
{
}


CppUnit::Test* MediaTypeTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("MediaTypeTest");

	CppUnit_addTest(pSuite, MediaTypeTest, testParse);
	CppUnit_addTest(pSuite, MediaTypeTest, testToString);
	CppUnit_addTest(pSuite, MediaTypeTest, testMatch);
	CppUnit_addTest(pSuite, MediaTypeTest, testMatchRange);

	return pSuite;
}
