//
// DateTimeParserTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/DateTimeParserTest.h#2 $
//
// Definition of the DateTimeParserTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DateTimeParserTest_INCLUDED
#define DateTimeParserTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class DateTimeParserTest: public CppUnit::TestCase
{
public:
	DateTimeParserTest(const std::string& name);
	~DateTimeParserTest();

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
	void testGuess();
	void testParseMonth();
	void testParseDayOfWeek();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // DateTimeParserTest_INCLUDED
