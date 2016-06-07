//
// HashTableTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/HashTableTest.h#1 $
//
// Definition of the HashTableTest class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef HashTableTest_INCLUDED
#define HashTableTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class HashTableTest: public CppUnit::TestCase
{
public:
	HashTableTest(const std::string& name);
	~HashTableTest();

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


#endif // HashTableTest_INCLUDED
