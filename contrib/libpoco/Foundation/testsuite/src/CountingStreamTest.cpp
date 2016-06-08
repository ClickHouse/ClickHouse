//
// CountingStreamTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/CountingStreamTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CountingStreamTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/CountingStream.h"
#include <sstream>


using Poco::CountingInputStream;
using Poco::CountingOutputStream;


CountingStreamTest::CountingStreamTest(const std::string& name): CppUnit::TestCase(name)
{
}


CountingStreamTest::~CountingStreamTest()
{
}


void CountingStreamTest::testInput()
{
	char c;
	std::istringstream istr1("foo");
	CountingInputStream ci1(istr1);
	while (ci1.good()) ci1.get(c);
	assert (ci1.lines() == 1);
	assert (ci1.chars() == 3);
	assert (ci1.pos() == 3);

	std::istringstream istr2("foo\nbar");
	CountingInputStream ci2(istr2);
	while (ci2.good()) ci2.get(c);
	assert (ci2.lines() == 2);
	assert (ci2.chars() == 7);
	assert (ci2.pos() == 3);

	std::istringstream istr3("foo\nbar\n");
	CountingInputStream ci3(istr3);
	while (ci3.good()) ci3.get(c);
	assert (ci3.lines() == 2);
	assert (ci3.chars() == 8);
	assert (ci3.pos() == 0);

	std::istringstream istr4("foo");
	CountingInputStream ci4(istr4);
	while (ci4.good()) ci4.get(c);
	ci4.addChars(10);
	ci4.addLines(2);
	ci4.addPos(3);
	assert (ci4.lines() == 1 + 2);
	assert (ci4.chars() == 3 + 10);
	assert (ci4.pos() == 3 + 3);
}


void CountingStreamTest::testOutput()
{
	std::ostringstream ostr1;
	CountingOutputStream co1(ostr1);
	co1 << "foo";
	assert (ostr1.str() == "foo");
	assert (co1.lines() == 1);
	assert (co1.chars() == 3);
	assert (co1.pos() == 3);

	std::ostringstream ostr2;
	CountingOutputStream co2(ostr2);
	co2 << "foo\nbar";
	assert (ostr2.str() == "foo\nbar");
	assert (co2.lines() == 2);
	assert (co2.chars() == 7);
	assert (co2.pos() == 3);

	CountingOutputStream co3;
	co3 << "foo\nbar\n";
	assert (co3.lines() == 2);
	assert (co3.chars() == 8);
	assert (co3.pos() == 0);

	std::ostringstream ostr4;
	CountingOutputStream co4(ostr4);
	co4 << "foo";
	co4.addChars(10);
	co4.addLines(2);
	co4.addPos(3);
	assert (co4.lines() == 1 + 2);
	assert (co4.chars() == 3 + 10);
	assert (co4.pos() == 3 + 3);
}


void CountingStreamTest::setUp()
{
}


void CountingStreamTest::tearDown()
{
}


CppUnit::Test* CountingStreamTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("CountingStreamTest");

	CppUnit_addTest(pSuite, CountingStreamTest, testInput);
	CppUnit_addTest(pSuite, CountingStreamTest, testOutput);

	return pSuite;
}
