//
// LRUCacheTest.h
//
// $Id: //poco/Main/Foundation/testsuite/src/LRUCacheTest.h#6 $
//
// Tests for LRUCache
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//

#ifndef LRUCacheTest_INCLUDED
#define LRUCacheTest_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/KeyValueArgs.h"
#include "CppUnit/TestCase.h"


class LRUCacheTest: public CppUnit::TestCase
{
public:
	LRUCacheTest(const std::string& name);
	~LRUCacheTest();

	void testClear();
	void testCacheSize0();
	void testCacheSize1();
	void testCacheSize2();
	void testCacheSizeN();
	void testDuplicateAdd();
	void testUpdate();

	void setUp();
	void tearDown();
	static CppUnit::Test* suite();

private:
	void onUpdate(const void* pSender, const Poco::KeyValueArgs<int, int>& args);
	void onAdd(const void* pSender, const Poco::KeyValueArgs<int, int>& args);
	void onRemove(const void* pSender, const int& args);

private:
	int addCnt;
	int updateCnt;
	int removeCnt;
};


#endif // LRUCacheTest_INCLUDED
