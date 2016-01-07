//
// SimpleHashTableTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/SimpleHashTableTest.h#1 $
//
// Definition of the SimpleHashTableTest class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef SimpleHashTableTest_INCLUDED
#define SimpleHashTableTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class SimpleHashTableTest: public CppUnit::TestCase
{
public:
	SimpleHashTableTest(const std::string& name);
	~SimpleHashTableTest();

	void testInsert();
	void testOverflow();
	void testUpdate();
	void testSize();
	void testResize();
	void testStatistic();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // SimpleHashTableTest_INCLUDED
