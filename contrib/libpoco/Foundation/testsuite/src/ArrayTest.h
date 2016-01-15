//
// ArrayTest.h
//
// $Id: //poco/svn/Foundation/testsuite/src/ArrayTest.h#2 $
//
// Definition of the ArrayTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef ArrayTest_INCLUDED
#define ArrayTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class ArrayTest: public CppUnit::TestCase
{
public:
	ArrayTest(const std::string& name);
	~ArrayTest();

	void testConstruction();
	void testOperations();
	void testContainer();
	void testIterator();
	void testAlgorithm();
	void testMultiLevelArray();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // ArrayTest_INCLUDED

