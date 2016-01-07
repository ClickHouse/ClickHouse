//
// LinearHashTableTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/LinearHashTableTest.cpp#1 $
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "LinearHashTableTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/LinearHashTable.h"
#include "Poco/HashTable.h"
#include "Poco/Stopwatch.h"
#include "Poco/NumberFormatter.h"
#include <set>
#include <iostream>


using Poco::LinearHashTable;
using Poco::Hash;
using Poco::HashTable;
using Poco::Stopwatch;
using Poco::NumberFormatter;


LinearHashTableTest::LinearHashTableTest(const std::string& name): CppUnit::TestCase(name)
{
}


LinearHashTableTest::~LinearHashTableTest()
{
}


void LinearHashTableTest::testInsert()
{
	const int N = 1000;

	LinearHashTable<int, Hash<int> > ht;
	
	assert (ht.empty());
	
	for (int i = 0; i < N; ++i)
	{
		std::pair<LinearHashTable<int, Hash<int> >::Iterator, bool> res = ht.insert(i);
		assert (*res.first == i);
		assert (res.second);
		LinearHashTable<int, Hash<int> >::Iterator it = ht.find(i);
		assert (it != ht.end());
		assert (*it == i);
		assert (ht.size() == i + 1);
	}		
	assert (ht.buckets() == N + 1);
	
	assert (!ht.empty());
	
	for (int i = 0; i < N; ++i)
	{
		LinearHashTable<int, Hash<int> >::Iterator it = ht.find(i);
		assert (it != ht.end());
		assert (*it == i);
	}
	
	for (int i = 0; i < N; ++i)
	{
		std::pair<LinearHashTable<int, Hash<int> >::Iterator, bool> res = ht.insert(i);
		assert (*res.first == i);
		assert (!res.second);
		assert (ht.size() == N);
		assert (ht.buckets() == N + 1);
	}		
}


void LinearHashTableTest::testErase()
{
	const int N = 1000;

	LinearHashTable<int, Hash<int> > ht;

	for (int i = 0; i < N; ++i)
	{
		ht.insert(i);
	}
	assert (ht.size() == N);
	
	for (int i = 0; i < N; i += 2)
	{
		ht.erase(i);
		LinearHashTable<int, Hash<int> >::Iterator it = ht.find(i);
		assert (it == ht.end());
	}
	assert (ht.size() == N/2);
	
	for (int i = 0; i < N; i += 2)
	{
		LinearHashTable<int, Hash<int> >::Iterator it = ht.find(i);
		assert (it == ht.end());
	}
	
	for (int i = 1; i < N; i += 2)
	{
		LinearHashTable<int, Hash<int> >::Iterator it = ht.find(i);
		assert (it != ht.end());
		assert (*it == i);
	}

	for (int i = 0; i < N; i += 2)
	{
		ht.insert(i);
	}
	
	for (int i = 0; i < N; ++i)
	{
		LinearHashTable<int, Hash<int> >::Iterator it = ht.find(i);
		assert (it != ht.end());
		assert (*it == i);
	}
}


void LinearHashTableTest::testIterator()
{
	const int N = 1000;

	LinearHashTable<int, Hash<int> > ht;

	for (int i = 0; i < N; ++i)
	{
		ht.insert(i);
	}
	
	std::set<int> values;
	LinearHashTable<int, Hash<int> >::Iterator it = ht.begin();
	while (it != ht.end())
	{
		assert (values.find(*it) == values.end());
		values.insert(*it);
		++it;
	}
	
	assert (values.size() == N);
}


void LinearHashTableTest::testConstIterator()
{
	const int N = 1000;

	LinearHashTable<int, Hash<int> > ht;

	for (int i = 0; i < N; ++i)
	{
		ht.insert(i);
	}

	std::set<int> values;
	LinearHashTable<int, Hash<int> >::ConstIterator it = ht.begin();
	while (it != ht.end())
	{
		assert (values.find(*it) == values.end());
		values.insert(*it);
		++it;
	}
	
	assert (values.size() == N);
	
	values.clear();
	const LinearHashTable<int, Hash<int> > cht(ht);

	LinearHashTable<int, Hash<int> >::ConstIterator cit = cht.begin();
	while (cit != cht.end())
	{
		assert (values.find(*cit) == values.end());
		values.insert(*cit);
		++cit;
	}
	
	assert (values.size() == N);	
}


void LinearHashTableTest::testPerformanceInt()
{
	const int N = 5000000;
	Stopwatch sw;

	{
		LinearHashTable<int, Hash<int> > lht(N);
		sw.start();
		for (int i = 0; i < N; ++i)
		{
			lht.insert(i);
		}
		sw.stop();
		std::cout << "Insert LHT: " << sw.elapsedSeconds() << std::endl;
		sw.reset();
		
		sw.start();
		for (int i = 0; i < N; ++i)
		{
			lht.find(i);
		}
		sw.stop();
		std::cout << "Find LHT: " << sw.elapsedSeconds() << std::endl;
		sw.reset();
	}

	{
		HashTable<int, int> ht;
		
		sw.start();
		for (int i = 0; i < N; ++i)
		{
			ht.insert(i, i);
		}
		sw.stop();
		std::cout << "Insert HT: " << sw.elapsedSeconds() << std::endl;
		sw.reset();
		
		sw.start();
		for (int i = 0; i < N; ++i)
		{
			ht.exists(i);
		}
		sw.stop();
		std::cout << "Find HT: " << sw.elapsedSeconds() << std::endl;
	}
	
	{
		std::set<int> s;
		sw.start();
		for (int i = 0; i < N; ++i)
		{
			s.insert(i);
		}
		sw.stop();
		std::cout << "Insert set: " << sw.elapsedSeconds() << std::endl;
		sw.reset();
		
		sw.start();
		for (int i = 0; i < N; ++i)
		{
			s.find(i);
		}
		sw.stop();
		std::cout << "Find set: " << sw.elapsedSeconds() << std::endl;
		sw.reset();
	}
	
}


void LinearHashTableTest::testPerformanceStr()
{
	const int N = 5000000;
	Stopwatch sw;
	
	std::vector<std::string> values;
	for (int i = 0; i < N; ++i)
	{
		values.push_back(NumberFormatter::format0(i, 8));
	}

	{
		LinearHashTable<std::string, Hash<std::string> > lht(N);
		sw.start();
		for (int i = 0; i < N; ++i)
		{
			lht.insert(values[i]);
		}
		sw.stop();
		std::cout << "Insert LHT: " << sw.elapsedSeconds() << std::endl;
		sw.reset();
		
		sw.start();
		for (int i = 0; i < N; ++i)
		{
			lht.find(values[i]);
		}
		sw.stop();
		std::cout << "Find LHT: " << sw.elapsedSeconds() << std::endl;
		sw.reset();
	}

	{
		HashTable<std::string, int> ht;
		
		sw.start();
		for (int i = 0; i < N; ++i)
		{
			ht.insert(values[i], i);
		}
		sw.stop();
		std::cout << "Insert HT: " << sw.elapsedSeconds() << std::endl;
		sw.reset();
		
		sw.start();
		for (int i = 0; i < N; ++i)
		{
			ht.exists(values[i]);
		}
		sw.stop();
		std::cout << "Find HT: " << sw.elapsedSeconds() << std::endl;
	}
	
	{
		std::set<std::string> s;
		sw.start();
		for (int i = 0; i < N; ++i)
		{
			s.insert(values[i]);
		}
		sw.stop();
		std::cout << "Insert set: " << sw.elapsedSeconds() << std::endl;
		sw.reset();
		
		sw.start();
		for (int i = 0; i < N; ++i)
		{
			s.find(values[i]);
		}
		sw.stop();
		std::cout << "Find set: " << sw.elapsedSeconds() << std::endl;
		sw.reset();
	}
}


void LinearHashTableTest::setUp()
{
}


void LinearHashTableTest::tearDown()
{
}


CppUnit::Test* LinearHashTableTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("LinearHashTableTest");

	CppUnit_addTest(pSuite, LinearHashTableTest, testInsert);
	CppUnit_addTest(pSuite, LinearHashTableTest, testErase);
	CppUnit_addTest(pSuite, LinearHashTableTest, testIterator);
	CppUnit_addTest(pSuite, LinearHashTableTest, testConstIterator);
	//CppUnit_addTest(pSuite, LinearHashTableTest, testPerformanceInt);
	//CppUnit_addTest(pSuite, LinearHashTableTest, testPerformanceStr);

	return pSuite;
}
