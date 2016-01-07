//
// HashSetTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/HashSetTest.cpp#1 $
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "HashSetTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/HashSet.h"
#include <set>


using Poco::Hash;
using Poco::HashSet;


HashSetTest::HashSetTest(const std::string& name): CppUnit::TestCase(name)
{
}


HashSetTest::~HashSetTest()
{
}


void HashSetTest::testInsert()
{
	const int N = 1000;

	HashSet<int, Hash<int> > hs;
	
	assert (hs.empty());
	
	for (int i = 0; i < N; ++i)
	{
		std::pair<HashSet<int, Hash<int> >::Iterator, bool> res = hs.insert(i);
		assert (*res.first == i);
		assert (res.second);
		HashSet<int, Hash<int> >::Iterator it = hs.find(i);
		assert (it != hs.end());
		assert (*it == i);
		assert (hs.size() == i + 1);
	}		
	
	assert (!hs.empty());
	
	for (int i = 0; i < N; ++i)
	{
		HashSet<int, Hash<int> >::Iterator it = hs.find(i);
		assert (it != hs.end());
		assert (*it == i);
	}
	
	for (int i = 0; i < N; ++i)
	{
		std::pair<HashSet<int, Hash<int> >::Iterator, bool> res = hs.insert(i);
		assert (*res.first == i);
		assert (!res.second);
	}		
}


void HashSetTest::testErase()
{
	const int N = 1000;

	HashSet<int, Hash<int> > hs;

	for (int i = 0; i < N; ++i)
	{
		hs.insert(i);
	}
	assert (hs.size() == N);
	
	for (int i = 0; i < N; i += 2)
	{
		hs.erase(i);
		HashSet<int, Hash<int> >::Iterator it = hs.find(i);
		assert (it == hs.end());
	}
	assert (hs.size() == N/2);
	
	for (int i = 0; i < N; i += 2)
	{
		HashSet<int, Hash<int> >::Iterator it = hs.find(i);
		assert (it == hs.end());
	}

	for (int i = 1; i < N; i += 2)
	{
		HashSet<int, Hash<int> >::Iterator it = hs.find(i);
		assert (it != hs.end());
		assert (*it == i);
	}

	for (int i = 0; i < N; i += 2)
	{
		hs.insert(i);
	}
	
	for (int i = 0; i < N; ++i)
	{
		HashSet<int, Hash<int> >::Iterator it = hs.find(i);
		assert (it != hs.end());
		assert (*it == i);
	}
}


void HashSetTest::testIterator()
{
	const int N = 1000;

	HashSet<int, Hash<int> > hs;

	for (int i = 0; i < N; ++i)
	{
		hs.insert(i);
	}
	
	std::set<int> values;
	HashSet<int, Hash<int> >::Iterator it = hs.begin();
	while (it != hs.end())
	{
		assert (values.find(*it) == values.end());
		values.insert(*it);
		++it;
	}

	assert (values.size() == N);
}


void HashSetTest::testConstIterator()
{
	const int N = 1000;

	HashSet<int, Hash<int> > hs;

	for (int i = 0; i < N; ++i)
	{
		hs.insert(i);
	}
	
	std::set<int> values;
	HashSet<int, Hash<int> >::ConstIterator it = hs.begin();
	while (it != hs.end())
	{
		assert (values.find(*it) == values.end());
		values.insert(*it);
		++it;
	}
	
	assert (values.size() == N);
}


void HashSetTest::setUp()
{
}


void HashSetTest::tearDown()
{
}


CppUnit::Test* HashSetTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("HashSetTest");

	CppUnit_addTest(pSuite, HashSetTest, testInsert);
	CppUnit_addTest(pSuite, HashSetTest, testErase);
	CppUnit_addTest(pSuite, HashSetTest, testIterator);
	CppUnit_addTest(pSuite, HashSetTest, testConstIterator);

	return pSuite;
}
