//
// NumberFormatterTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/NumberFormatterTest.h#1 $
//
// Definition of the NumberFormatterTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NumberFormatterTest_INCLUDED
#define NumberFormatterTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class NumberFormatterTest: public CppUnit::TestCase
{
public:
	NumberFormatterTest(const std::string& name);
	~NumberFormatterTest();

	void testFormat();
	void testFormat0();
	void testFormatBool();
	void testFormatHex();
	void testFormatFloat();
	void testAppend();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // NumberFormatterTest_INCLUDED
