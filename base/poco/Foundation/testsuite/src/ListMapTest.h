//
// ListMapTest.h
//
// Definition of the ListMapTest class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef ListMapTest_INCLUDED
#define ListMapTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class ListMapTest: public CppUnit::TestCase
{
public:
	ListMapTest(const std::string& name);
	~ListMapTest();

	void testInsert();
	void testInsertOrder();
	void testErase();
	void testIterator();
	void testConstIterator();
	void testIntIndex();
	void testStringIndex();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // ListMapTest_INCLUDED
