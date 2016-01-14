//
// NullStreamTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/NullStreamTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "NullStreamTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/NullStream.h"


using Poco::NullInputStream;
using Poco::NullOutputStream;


NullStreamTest::NullStreamTest(const std::string& name): CppUnit::TestCase(name)
{
}


NullStreamTest::~NullStreamTest()
{
}


void NullStreamTest::testInput()
{
	NullInputStream istr;
	assert (istr.good());
	assert (!istr.eof());
	int c = istr.get();
	assert (c == -1);
	assert (istr.eof());
}


void NullStreamTest::testOutput()
{
	NullOutputStream ostr;
	assert (ostr.good());
	ostr << "Hello, world!";
	assert (ostr.good());
}


void NullStreamTest::setUp()
{
}


void NullStreamTest::tearDown()
{
}


CppUnit::Test* NullStreamTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("NullStreamTest");

	CppUnit_addTest(pSuite, NullStreamTest, testInput);
	CppUnit_addTest(pSuite, NullStreamTest, testOutput);

	return pSuite;
}
