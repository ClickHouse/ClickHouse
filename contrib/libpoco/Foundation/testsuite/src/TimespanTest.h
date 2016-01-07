//
// TimespanTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/TimespanTest.h#1 $
//
// Definition of the TimespanTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef TimespanTest_INCLUDED
#define TimespanTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class TimespanTest: public CppUnit::TestCase
{
public:
	TimespanTest(const std::string& name);
	~TimespanTest();

	void testConversions();
	void testComparisons();
	void testArithmetics();
	void testSwap();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // TimespanTest_INCLUDED
