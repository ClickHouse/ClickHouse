//
// ListMapTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/ListMapTest.cpp#1 $
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "ListMapTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/ListMap.h"
#include "Poco/Exception.h"
#include <map>

GCC_DIAG_OFF(unused-variable)

using Poco::ListMap;


ListMapTest::ListMapTest(const std::string& name): CppUnit::TestCase(name)
{
}


ListMapTest::~ListMapTest()
{
}


void ListMapTest::testInsert()
{
	const int N = 1000;

	typedef ListMap<int, int> IntMap;
	IntMap hm;
	
	assert (hm.empty());
	
	for (int i = 0; i < N; ++i)
	{
		IntMap::Iterator res = hm.insert(IntMap::ValueType(i, i*2));
		assert (res->first == i);
		assert (res->second == i*2);
		IntMap::Iterator it = hm.find(i);
		assert (it != hm.end());
		assert (it->first == i);
		assert (it->second == i*2);
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
	
	hm.clear();
	for (int i = 0; i < N; ++i)
	{
		IntMap::Iterator res = hm.insert(IntMap::ValueType(i, 0));
		assert (res->first == i);
		assert (res->second == 0);
	}		
}


void ListMapTest::testErase()
{
	const int N = 1000;

	typedef ListMap<int, int> IntMap;
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
		assert (it->first == i);
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


void ListMapTest::testIterator()
{
	const int N = 1000;

	typedef ListMap<int, int> IntMap;
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


void ListMapTest::testConstIterator()
{
	const int N = 1000;

	typedef ListMap<int, int> IntMap;
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


void ListMapTest::testIntIndex()
{
	typedef ListMap<int, int> IntMap;
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


void ListMapTest::testStringIndex()
{
	typedef ListMap<const char*, std::string> StringMap;
	StringMap hm;

	hm["index1"] = "value2";
	hm["index2"] = "value4";
	hm["index3"] = "value6";
	
	assert (hm.size() == 3);
	assert (hm["index1"] == "value2");
	assert (hm["Index2"] == "value4");
	assert (hm["inDeX3"] == "value6");
	
	try
	{
		const StringMap& im = hm;
		std::string x = im["index4"];
		fail("no such key - must throw");
	}
	catch (Poco::NotFoundException&)
	{
	}
}


void ListMapTest::setUp()
{
}


void ListMapTest::tearDown()
{
}


CppUnit::Test* ListMapTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("ListMapTest");

	CppUnit_addTest(pSuite, ListMapTest, testInsert);
	CppUnit_addTest(pSuite, ListMapTest, testErase);
	CppUnit_addTest(pSuite, ListMapTest, testIterator);
	CppUnit_addTest(pSuite, ListMapTest, testConstIterator);
	CppUnit_addTest(pSuite, ListMapTest, testIntIndex);
	CppUnit_addTest(pSuite, ListMapTest, testStringIndex);

	return pSuite;
}
