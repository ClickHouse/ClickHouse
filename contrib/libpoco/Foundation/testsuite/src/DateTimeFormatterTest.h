//
// DateTimeFormatterTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/DateTimeFormatterTest.h#2 $
//
// Definition of the DateTimeFormatterTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DateTimeFormatterTest_INCLUDED
#define DateTimeFormatterTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class DateTimeFormatterTest: public CppUnit::TestCase
{
public:
	DateTimeFormatterTest(const std::string& name);
	~DateTimeFormatterTest();

	void testISO8601();
	void testISO8601Frac();
	void testRFC822();
	void testRFC1123();
	void testHTTP();
	void testRFC850();
	void testRFC1036();
	void testASCTIME();
	void testSORTABLE();
	void testCustom();
	void testTimespan();
	
	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // DateTimeFormatterTest_INCLUDED
