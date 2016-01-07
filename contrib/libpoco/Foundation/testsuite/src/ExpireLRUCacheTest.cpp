//
// ExpireLRUCacheTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/ExpireLRUCacheTest.cpp#1 $
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "ExpireLRUCacheTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Exception.h"
#include "Poco/ExpireLRUCache.h"
#include "Poco/AccessExpireLRUCache.h"
#include "Poco/Bugcheck.h"
#include "Poco/Thread.h"


using namespace Poco;


#define DURSLEEP 250
#define DURHALFSLEEP DURSLEEP / 2
#define DURWAIT  300


ExpireLRUCacheTest::ExpireLRUCacheTest(const std::string& name): CppUnit::TestCase(name)
{
}


ExpireLRUCacheTest::~ExpireLRUCacheTest()
{
}


void ExpireLRUCacheTest::testClear()
{
	ExpireLRUCache<int, int> aCache(DURSLEEP);
	aCache.add(1, 2);
	aCache.add(3, 4);
	aCache.add(5, 6);
	assert (aCache.has(1));
	assert (aCache.has(3));
	assert (aCache.has(5));
	assert (*aCache.get(1) == 2);
	assert (*aCache.get(3) == 4);
	assert (*aCache.get(5) == 6);
	aCache.clear();
	assert (!aCache.has(1));
	assert (!aCache.has(3));
	assert (!aCache.has(5));
}


void ExpireLRUCacheTest::testExpire0()
{
	try
	{
		ExpireLRUCache<int, int> aCache(1024, 24);
		failmsg("cache expire lower than 25 is illegal, test should fail");
	}
	catch (Poco::InvalidArgumentException&)
	{
	}
}


void ExpireLRUCacheTest::testExpireN()
{
	// 3-1 represents the cache sorted by age, elements get replaced at the end of the list
	// 3-1|5 -> 5 gets removed
	ExpireLRUCache<int, int> aCache(3, DURSLEEP);
	aCache.add(1, 2); // 1
	assert (aCache.has(1));
	SharedPtr<int> tmp = aCache.get(1);
	assert (!tmp.isNull());
	assert (*tmp == 2);
	Thread::sleep(DURWAIT);
	assert (!aCache.has(1));

	// tmp must still be valid, access it
	assert (*tmp == 2);
	tmp = aCache.get(1);
	assert (!tmp);

	aCache.add(1, 2); // 1
	Thread::sleep(DURHALFSLEEP);
	aCache.add(3, 4); // 3-1
	assert (aCache.has(1));
	assert (aCache.has(3));
	tmp = aCache.get(1);
	SharedPtr<int> tmp2 = aCache.get(3);
	assert (*tmp == 2); 
	assert (*tmp2 == 4);

	Thread::sleep(DURHALFSLEEP+25); //3|1
	assert (!aCache.has(1));
	assert (aCache.has(3));
	assert (*tmp == 2); // 1-3
	assert (*tmp2 == 4); // 3-1
	tmp2 = aCache.get(3);
	assert (*tmp2 == 4);
	Thread::sleep(DURHALFSLEEP+25); //3|1
	assert (!aCache.has(3));
	assert (*tmp2 == 4);
	tmp = aCache.get(1);
	tmp2 = aCache.get(3);
	assert (!tmp);
	assert (!tmp2);

	// removing illegal entries should work too
	aCache.remove(666);

	aCache.clear();
	assert (!aCache.has(5));
	assert (!aCache.has(3));
}


void ExpireLRUCacheTest::testAccessExpireN()
{
	// 3-1 represents the cache sorted by age, elements get replaced at the end of the list
	// 3-1|5 -> 5 gets removed
	AccessExpireLRUCache<int, int> aCache(3, DURSLEEP);
	aCache.add(1, 2); // 1
	assert (aCache.has(1));
	SharedPtr<int> tmp = aCache.get(1);
	assert (!tmp.isNull());
	assert (*tmp == 2);
	assert (aCache.size() == 1);
	Thread::sleep(DURWAIT);
	assert (aCache.size() == 0);
	assert (!aCache.has(1));

	// tmp must still be valid, access it
	assert (*tmp == 2);
	tmp = aCache.get(1);
	assert (!tmp);

	aCache.add(1, 2); // 1
	Thread::sleep(DURHALFSLEEP);
	aCache.add(3, 4); // 3-1
	assert (aCache.has(1));
	assert (aCache.has(3));

	Thread::sleep(DURHALFSLEEP+50); //3|1
	assert (!aCache.has(1));
	assert (*aCache.get(3) == 4);
	Thread::sleep(DURHALFSLEEP+25); //3|1
	assert (*aCache.get(3) == 4);
	// removing illegal entries should work too
	aCache.remove(666);

	aCache.clear();
	assert (!aCache.has(5));
	assert (!aCache.has(3));
}


void ExpireLRUCacheTest::testCacheSize0()
{
	// cache size 0 is illegal
	try
	{
		ExpireLRUCache<int, int> aCache(0);
		failmsg ("cache size of 0 is illegal, test should fail");
	}
	catch (Poco::InvalidArgumentException&)
	{
	}
}


void ExpireLRUCacheTest::testCacheSize1()
{
	ExpireLRUCache<int, int> aCache(1);
	aCache.add(1, 2);
	assert (aCache.has(1));
	assert (*aCache.get(1) == 2);

	aCache.add(3, 4); // replaces 1
	assert (!aCache.has(1));
	assert (aCache.has(3));
	assert (*aCache.get(3) == 4);

	aCache.add(5, 6);
	assert (!aCache.has(1));
	assert (!aCache.has(3));
	assert (aCache.has(5));
	assert (*aCache.get(5) == 6);

	aCache.remove(5);
	assert (!aCache.has(5));

	// removing illegal entries should work too
	aCache.remove(666);
}


void ExpireLRUCacheTest::testCacheSize2()
{
	// 3-1 represents the cache sorted by pos, elements get replaced at the end of the list
	// 3-1|5 -> 5 gets removed
	ExpireLRUCache<int, int> aCache(2);
	aCache.add(1, 2); // 1
	assert (aCache.has(1));
	assert (*aCache.get(1) == 2);

	aCache.add(3, 4); // 3-1
	assert (aCache.has(1));
	assert (aCache.has(3));
	assert (*aCache.get(1) == 2); // 1-3
	assert (*aCache.get(3) == 4); // 3-1

	aCache.add(5, 6); // 5-3|1
	assert (!aCache.has(1));
	assert (aCache.has(3));
	assert (aCache.has(5));
	assert (*aCache.get(5) == 6);  // 5-3
	assert (*aCache.get(3) == 4);  // 3-5

	// test remove from the end and the beginning of the list
	aCache.remove(5); // 3
	assert (!aCache.has(5));
	assert (*aCache.get(3) == 4);  // 3
	aCache.add(5, 6); // 5-3
	assert (*aCache.get(3) == 4);  // 3-5
	aCache.remove(3); // 5
	assert (!aCache.has(3));
	assert (*aCache.get(5) == 6);  // 5

	// removing illegal entries should work too
	aCache.remove(666);

	aCache.clear();
	assert (!aCache.has(5));
}


void ExpireLRUCacheTest::testCacheSizeN()
{
		// 3-1 represents the cache sorted by pos, elements get replaced at the end of the list
	// 3-1|5 -> 5 gets removed
	ExpireLRUCache<int, int> aCache(3);
	aCache.add(1, 2); // 1
	assert (aCache.has(1));
	assert (*aCache.get(1) == 2);

	aCache.add(3, 4); // 3-1
	assert (aCache.has(1));
	assert (aCache.has(3));
	assert (*aCache.get(1) == 2); // 1-3
	assert (*aCache.get(3) == 4); // 3-1

	aCache.add(5, 6); // 5-3-1
	assert (aCache.has(1));
	assert (aCache.has(3));
	assert (aCache.has(5));
	assert (*aCache.get(5) == 6);  // 5-3-1
	assert (*aCache.get(3) == 4);  // 3-5-1

	aCache.add(7, 8); // 7-5-3|1
	assert (!aCache.has(1));
	assert (aCache.has(7));
	assert (aCache.has(3));
	assert (aCache.has(5));
	assert (*aCache.get(5) == 6);  // 5-7-3
	assert (*aCache.get(3) == 4);  // 3-5-7
	assert (*aCache.get(7) == 8);  // 7-3-5

	// test remove from the end and the beginning of the list
	aCache.remove(5); // 7-3
	assert (!aCache.has(5));
	assert (*aCache.get(3) == 4);  // 3-7
	aCache.add(5, 6); // 5-3-7
	assert (*aCache.get(7) == 8);  // 7-5-3
	aCache.remove(7); // 5-3
	assert (!aCache.has(7));
	assert (aCache.has(3));
	assert (*aCache.get(5) == 6);  // 5-3

	// removing illegal entries should work too
	aCache.remove(666);

	aCache.clear();
	assert (!aCache.has(5));
	assert (!aCache.has(3));
}


void ExpireLRUCacheTest::testDuplicateAdd()
{
	ExpireLRUCache<int, int> aCache(3);
	aCache.add(1, 2); // 1
	assert (aCache.has(1));
	assert (*aCache.get(1) == 2);
	aCache.add(1, 3);
	assert (aCache.has(1));
	assert (*aCache.get(1) == 3);
}


void ExpireLRUCacheTest::setUp()
{
}


void ExpireLRUCacheTest::tearDown()
{
}


CppUnit::Test* ExpireLRUCacheTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("ExpireLRUCacheTest");

	CppUnit_addTest(pSuite, ExpireLRUCacheTest, testClear);
	CppUnit_addTest(pSuite, ExpireLRUCacheTest, testExpire0);
	CppUnit_addTest(pSuite, ExpireLRUCacheTest, testExpireN);
	CppUnit_addTest(pSuite, ExpireLRUCacheTest, testAccessExpireN);
	CppUnit_addTest(pSuite, ExpireLRUCacheTest, testCacheSize0);
	CppUnit_addTest(pSuite, ExpireLRUCacheTest, testCacheSize1);
	CppUnit_addTest(pSuite, ExpireLRUCacheTest, testCacheSize2);
	CppUnit_addTest(pSuite, ExpireLRUCacheTest, testCacheSizeN);
	CppUnit_addTest(pSuite, ExpireLRUCacheTest, testDuplicateAdd);

	return pSuite;
}
