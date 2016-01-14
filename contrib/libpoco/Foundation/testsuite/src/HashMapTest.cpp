//
// HashMapTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/HashMapTest.cpp#1 $
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "HashMapTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/HashMap.h"
#include "Poco/Exception.h"
#include <map>

GCC_DIAG_OFF(unused-variable)

using Poco::HashMap;


HashMapTest::HashMapTest(const std::string& name): CppUnit::TestCase(name)
{
}


HashMapTest::~HashMapTest()
{
}


void HashMapTest::testInsert()
{
	const int N = 1000;

	typedef HashMap<int, int> IntMap;
	IntMap hm;
	
	assert (hm.empty());
	
	for (int i = 0; i < N; ++i)
	{
		std::pair<IntMap::Iterator, bool> res = hm.insert(IntMap::ValueType(i, i*2));
		assert (res.first->first == i);
		assert (res.first->second == i*2);
		assert (res.second);
		IntMap::Iterator it = hm.find(i);
		assert (it != hm.end());
		assert (it->first == i);
		assert (it->second == i*2);
		assert (hm.count(i) == 1);
		assert (hm.size() == i + 1);
	}		
	
	assert (!hm.empty());
	
	for (int i = 0; i < N; ++i)
	{
		IntMap::Iterator it = hm.find(i);
		assert (it != hm.end());
		assert (it->first == i);
		assert (it->second == i*2);
	}
	
	for (int i = 0; i < N; ++i)
	{
		std::pair<IntMap::Iterator, bool> res = hm.insert(IntMap::ValueType(i, 0));
		assert (res.first->first == i);
		assert (res.first->second == i*2);
		assert (!res.second);
	}		
}


void HashMapTest::testErase()
{
	const int N = 1000;

	typedef HashMap<int, int> IntMap;
	IntMap hm;

	for (int i = 0; i < N; ++i)
	{
		hm.insert(IntMap::ValueType(i, i*2));
	}
	assert (hm.size() == N);
	
	for (int i = 0; i < N; i += 2)
	{
		hm.erase(i);
		IntMap::Iterator it = hm.find(i);
		assert (it == hm.end());
	}
	assert (hm.size() == N/2);
	
	for (int i = 0; i < N; i += 2)
	{
		IntMap::Iterator it = hm.find(i);
		assert (it == hm.end());
	}
	
	for (int i = 1; i < N; i += 2)
	{
		IntMap::Iterator it = hm.find(i);
		assert (it != hm.end());
		assert (*it == i);
	}

	for (int i = 0; i < N; i += 2)
	{
		hm.insert(IntMap::ValueType(i, i*2));
	}
	
	for (int i = 0; i < N; ++i)
	{
		IntMap::Iterator it = hm.find(i);
		assert (it != hm.end());
		assert (it->first == i);
		assert (it->second == i*2);		
	}
}


void HashMapTest::testIterator()
{
	const int N = 1000;

	typedef HashMap<int, int> IntMap;
	IntMap hm;

	for (int i = 0; i < N; ++i)
	{
		hm.insert(IntMap::ValueType(i, i*2));
	}
	
	std::map<int, int> values;
	IntMap::Iterator it; // do not initialize here to test proper behavior of uninitialized iterators
	it = hm.begin();
	while (it != hm.end())
	{
		assert (values.find(it->first) == values.end());
		values[it->first] = it->second;
		++it;
	}
	
	assert (values.size() == N);
}


void HashMapTest::testConstIterator()
{
	const int N = 1000;

	typedef HashMap<int, int> IntMap;
	IntMap hm;

	for (int i = 0; i < N; ++i)
	{
		hm.insert(IntMap::ValueType(i, i*2));
	}
	
	std::map<int, int> values;
	IntMap::ConstIterator it = hm.begin();
	while (it != hm.end())
	{
		assert (values.find(it->first) == values.end());
		values[it->first] = it->second;
		++it;
	}
	
	assert (values.size() == N);
}


void HashMapTest::testIndex()
{
	typedef HashMap<int, int> IntMap;
	IntMap hm;

	hm[1] = 2;
	hm[2] = 4;
	hm[3] = 6;
	
	assert (hm.size() == 3);
	assert (hm[1] == 2);
	assert (hm[2] == 4);
	assert (hm[3] == 6);
	
	try
	{
		const IntMap& im = hm;
		int x = im[4];
		fail("no such key - must throw");
	}
	catch (Poco::NotFoundException&)
	{
	}
}


void HashMapTest::setUp()
{
}


void HashMapTest::tearDown()
{
}


CppUnit::Test* HashMapTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("HashMapTest");

	CppUnit_addTest(pSuite, HashMapTest, testInsert);
	CppUnit_addTest(pSuite, HashMapTest, testErase);
	CppUnit_addTest(pSuite, HashMapTest, testIterator);
	CppUnit_addTest(pSuite, HashMapTest, testConstIterator);
	CppUnit_addTest(pSuite, HashMapTest, testIndex);

	return pSuite;
}
