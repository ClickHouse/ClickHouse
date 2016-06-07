//
// ExpireLRUCacheTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/ExpireLRUCacheTest.h#1 $
//
// Tests for ExpireLRUCache
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//

#ifndef ExpireLRUCacheTest_INCLUDED
#define ExpireLRUCacheTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class ExpireLRUCacheTest: public CppUnit::TestCase
{
public:
	ExpireLRUCacheTest(const std::string& name);
	~ExpireLRUCacheTest();

	void testClear();
	void testExpire0();
	void testExpireN();
	void testAccessExpireN();
	void testCacheSize0();
	void testCacheSize1();
	void testCacheSize2();
	void testCacheSizeN();
	void testDuplicateAdd();
	
	void setUp();
	void tearDown();
	static CppUnit::Test* suite();
};


#endif // ExpireLRUCacheTest_INCLUDED
