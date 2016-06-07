//
// FormatTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/FormatTest.h#1 $
//
// Definition of the FormatTest class.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef FormatTest_INCLUDED
#define FormatTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class FormatTest: public CppUnit::TestCase
{
public:
	FormatTest(const std::string& name);
	~FormatTest();

	void testChar();
	void testInt();
	void testBool();
	void testAnyInt();
	void testFloatFix();
	void testFloatSci();
	void testString();
	void testMultiple();
	void testIndex();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // FormatTest_INCLUDED
