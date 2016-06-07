//
// ExpireCacheTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/ExpireCacheTest.h#1 $
//
// Tests for ExpireCache
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//

#ifndef ExpireCacheTest_INCLUDED
#define ExpireCacheTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class ExpireCacheTest: public CppUnit::TestCase
{
public:
	ExpireCacheTest(const std::string& name);
	~ExpireCacheTest();

	void testClear();
	void testDuplicateAdd();
	void testExpire0();
	void testExpireN();
	void testAccessExpireN();
	void testExpireWithHas();

	
	void setUp();
	void tearDown();
	static CppUnit::Test* suite();
};


#endif // ExpireCacheTest_INCLUDED
