//
// UniqueExpireLRUCacheTest.cpp
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "UniqueExpireLRUCacheTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Exception.h"
#include "Poco/UniqueExpireLRUCache.h"
#include "Poco/UniqueAccessExpireLRUCache.h"
#include "Poco/AccessExpirationDecorator.h"
#include "Poco/Bugcheck.h"
#include "Poco/Thread.h"


using namespace Poco;


struct IntVal
{
	int value;
	Poco::Timestamp validUntil;
	IntVal(int val, Poco::Timestamp::TimeDiff v):value(val), validUntil()
	{
		validUntil += (v*1000);
	}

	const Poco::Timestamp& getExpiration() const
	{
		return validUntil;
	}
};


typedef AccessExpirationDecorator<int> DIntVal;


#define DURSLEEP 250
#define DURHALFSLEEP DURSLEEP / 2
#define DURWAIT  300


UniqueExpireLRUCacheTest::UniqueExpireLRUCacheTest(const std::string& name): CppUnit::TestCase(name)
{
}


UniqueExpireLRUCacheTest::~UniqueExpireLRUCacheTest()
{
}


void UniqueExpireLRUCacheTest::testClear()
{
	UniqueExpireLRUCache<int, IntVal> aCache;
	aCache.add(1, IntVal(2, DURSLEEP));
	aCache.add(3, IntVal(4, DURSLEEP));
	aCache.add(5, IntVal(6, DURSLEEP));
	assert (aCache.has(1));
	assert (aCache.has(3));
	assert (aCache.has(5));
	assert (aCache.get(1)->value == 2);
	assert (aCache.get(3)->value == 4);
	assert (aCache.get(5)->value == 6);
	aCache.clear();
	assert (!aCache.has(1));
	assert (!aCache.has(3));
	assert (!aCache.has(5));
}



void UniqueExpireLRUCacheTest::testAccessClear()
{
	UniqueAccessExpireLRUCache<int, DIntVal> aCache;
	aCache.add(1, DIntVal(2, DURSLEEP));
	aCache.add(3, DIntVal(4, DURSLEEP));
	aCache.add(5, DIntVal(6, DURSLEEP));
	assert (aCache.has(1));
	assert (aCache.has(3));
	assert (aCache.has(5));
	assert (aCache.get(1)->value() == 2);
	assert (aCache.get(3)->value() == 4);
	assert (aCache.get(5)->value() == 6);
	aCache.clear();
	assert (!aCache.has(1));
	assert (!aCache.has(3));
	assert (!aCache.has(5));
}

void UniqueExpireLRUCacheTest::testExpire0()
{
	UniqueExpireLRUCache<int, IntVal> aCache;
	aCache.add(1, IntVal(2, 0));
	assert (!aCache.has(1));
}


void UniqueExpireLRUCacheTest::testExpireN()
{
	// 3-1 represents the cache sorted by age, elements get replaced at the end of the list
	// 3-1|5 -> 5 gets removed
	UniqueExpireLRUCache<int, IntVal> aCache(3);
	aCache.add(1, IntVal(2, DURSLEEP)); // 1
	assert (aCache.has(1));
	SharedPtr<IntVal> tmp = aCache.get(1);
	assert (!tmp.isNull());
	assert (tmp->value == 2);
	Thread::sleep(DURWAIT);
	assert (!aCache.has(1));

	// tmp must still be valid, access it
	assert (tmp->value == 2);
	tmp = aCache.get(1);
	assert (!tmp);

	aCache.add(1, IntVal(2, DURSLEEP)); // 1
	Thread::sleep(DURHALFSLEEP);
	aCache.add(3, IntVal(4, DURSLEEP)); // 3-1
	assert (aCache.has(1));
	assert (aCache.has(3));
	tmp = aCache.get(1);
	SharedPtr<IntVal> tmp2 = aCache.get(3);
	assert (tmp->value == 2); 
	assert (tmp2->value == 4);

	Thread::sleep(DURHALFSLEEP+25); //3|1
	assert (!aCache.has(1));
	assert (aCache.has(3));
	assert (tmp->value == 2); // 1-3
	assert (tmp2->value == 4); // 3-1
	tmp2 = aCache.get(3);
	assert (tmp2->value == 4);
	Thread::sleep(DURHALFSLEEP+25); //3|1
	assert (!aCache.has(3));
	assert (tmp2->value == 4);
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


void UniqueExpireLRUCacheTest::testCacheSize0()
{
	// cache size 0 is illegal
	try
	{
		UniqueExpireLRUCache<int, IntVal> aCache(0);
		failmsg ("cache size of 0 is illegal, test should fail");
	}
	catch (Poco::InvalidArgumentException&)
	{
	}
}


void UniqueExpireLRUCacheTest::testCacheSize1()
{
	UniqueExpireLRUCache<int, IntVal> aCache(1);
	aCache.add(1, IntVal(2, DURSLEEP));
	assert (aCache.has(1));
	assert (aCache.get(1)->value == 2);

	aCache.add(3, IntVal(4, DURSLEEP)); // replaces 1
	assert (!aCache.has(1));
	assert (aCache.has(3));
	assert (aCache.get(3)->value == 4);

	aCache.add(5, IntVal(6, DURSLEEP));
	assert (!aCache.has(1));
	assert (!aCache.has(3));
	assert (aCache.has(5));
	assert (aCache.get(5)->value == 6);

	aCache.remove(5);
	assert (!aCache.has(5));

	// removing illegal entries should work too
	aCache.remove(666);
}


void UniqueExpireLRUCacheTest::testCacheSize2()
{
	// 3-1 represents the cache sorted by pos, elements get replaced at the end of the list
	// 3-1|5 -> 5 gets removed
	UniqueExpireLRUCache<int, IntVal> aCache(2);
	aCache.add(1, IntVal(2, DURSLEEP)); // 1
	assert (aCache.has(1));
	assert (aCache.get(1)->value == 2);

	aCache.add(3, IntVal(4, DURSLEEP)); // 3-1
	assert (aCache.has(1));
	assert (aCache.has(3));
	assert (aCache.get(1)->value == 2); // 1-3
	assert (aCache.get(3)->value == 4); // 3-1

	aCache.add(5, IntVal(6, DURSLEEP)); // 5-3|1
	assert (!aCache.has(1));
	assert (aCache.has(3));
	assert (aCache.has(5));
	assert (aCache.get(5)->value == 6);  // 5-3
	assert (aCache.get(3)->value == 4);  // 3-5

	// test remove from the end and the beginning of the list
	aCache.remove(5); // 3
	assert (!aCache.has(5));
	assert (aCache.get(3)->value == 4);  // 3
	aCache.add(5, IntVal(6, DURSLEEP)); // 5-3
	assert (aCache.get(3)->value == 4);  // 3-5
	aCache.remove(3); // 5
	assert (!aCache.has(3));
	assert (aCache.get(5)->value == 6);  // 5

	// removing illegal entries should work too
	aCache.remove(666);

	aCache.clear();
	assert (!aCache.has(5));
}


void UniqueExpireLRUCacheTest::testCacheSizeN()
{
		// 3-1 represents the cache sorted by pos, elements get replaced at the end of the list
	// 3-1|5 -> 5 gets removed
	UniqueExpireLRUCache<int, IntVal> aCache(3);
	aCache.add(1, IntVal(2, DURSLEEP)); // 1
	assert (aCache.has(1));
	assert (aCache.get(1)->value == 2);

	aCache.add(3, IntVal(4, DURSLEEP)); // 3-1
	assert (aCache.has(1));
	assert (aCache.has(3));
	assert (aCache.get(1)->value == 2); // 1-3
	assert (aCache.get(3)->value == 4); // 3-1

	aCache.add(5, IntVal(6, DURSLEEP)); // 5-3-1
	assert (aCache.has(1));
	assert (aCache.has(3));
	assert (aCache.has(5));
	assert (aCache.get(5)->value == 6);  // 5-3-1
	assert (aCache.get(3)->value == 4);  // 3-5-1

	aCache.add(7, IntVal(8, DURSLEEP)); // 7-5-3|1
	assert (!aCache.has(1));
	assert (aCache.has(7));
	assert (aCache.has(3));
	assert (aCache.has(5));
	assert (aCache.get(5)->value == 6);  // 5-7-3
	assert (aCache.get(3)->value == 4);  // 3-5-7
	assert (aCache.get(7)->value == 8);  // 7-3-5

	// test remove from the end and the beginning of the list
	aCache.remove(5); // 7-3
	assert (!aCache.has(5));
	assert (aCache.get(3)->value == 4);  // 3-7
	aCache.add(5, IntVal(6, DURSLEEP)); // 5-3-7
	assert (aCache.get(7)->value == 8);  // 7-5-3
	aCache.remove(7); // 5-3
	assert (!aCache.has(7));
	assert (aCache.has(3));
	assert (aCache.get(5)->value == 6);  // 5-3

	// removing illegal entries should work too
	aCache.remove(666);

	aCache.clear();
	assert (!aCache.has(5));
	assert (!aCache.has(3));
}


void UniqueExpireLRUCacheTest::testDuplicateAdd()
{
	UniqueExpireLRUCache<int, IntVal> aCache(3);
	aCache.add(1, IntVal(2, DURSLEEP)); // 1
	assert (aCache.has(1));
	assert (aCache.get(1)->value == 2);
	aCache.add(1, IntVal(3, DURSLEEP));
	assert (aCache.has(1));
	assert (aCache.get(1)->value == 3);
}


void UniqueExpireLRUCacheTest::setUp()
{
}


void UniqueExpireLRUCacheTest::tearDown()
{
}


CppUnit::Test* UniqueExpireLRUCacheTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("UniqueExpireLRUCacheTest");

	CppUnit_addTest(pSuite, UniqueExpireLRUCacheTest, testClear);
	CppUnit_addTest(pSuite, UniqueExpireLRUCacheTest, testAccessClear);
	CppUnit_addTest(pSuite, UniqueExpireLRUCacheTest, testExpire0);
	CppUnit_addTest(pSuite, UniqueExpireLRUCacheTest, testExpireN);
	CppUnit_addTest(pSuite, UniqueExpireLRUCacheTest, testCacheSize0);
	CppUnit_addTest(pSuite, UniqueExpireLRUCacheTest, testCacheSize1);
	CppUnit_addTest(pSuite, UniqueExpireLRUCacheTest, testCacheSize2);
	CppUnit_addTest(pSuite, UniqueExpireLRUCacheTest, testCacheSizeN);
	CppUnit_addTest(pSuite, UniqueExpireLRUCacheTest, testDuplicateAdd);

	return pSuite;
}
