//
// SimpleHashTableTest.cpp
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "SimpleHashTableTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/SimpleHashTable.h"
#include "Poco/NumberFormatter.h"


using namespace Poco;


SimpleHashTableTest::SimpleHashTableTest(const std::string& name): CppUnit::TestCase(name)
{
}


SimpleHashTableTest::~SimpleHashTableTest()
{
}


void SimpleHashTableTest::testInsert()
{
	std::string s1("str1");
	std::string s2("str2");
	SimpleHashTable<std::string, int> hashTable;
	assert (!hashTable.exists(s1));
	hashTable.insert(s1, 13);
	assert (hashTable.exists(s1));
	assert (hashTable.get(s1) == 13);
	int retVal = 0;

	assert (hashTable.get(s1, retVal));
	assert (retVal == 13);
	try
	{
		hashTable.insert(s1, 22);
		failmsg ("duplicate insert must fail");
	}
	catch (Exception&){}
	try
	{
		hashTable.get(s2);
		failmsg ("getting a non inserted item must fail");
	}
	catch (Exception&){}

	assert (!hashTable.exists(s2));
	hashTable.insert(s2, 13);
	assert (hashTable.exists(s2));
}


void SimpleHashTableTest::testUpdate()
{
	// add code for second test here
	std::string s1("str1");
	std::string s2("str2");
	SimpleHashTable<std::string, int> hashTable;
	hashTable.insert(s1, 13);
	hashTable.update(s1, 14);
	assert (hashTable.exists(s1));
	assert (hashTable.get(s1) == 14);
	int retVal = 0;

	assert (hashTable.get(s1, retVal));
	assert (retVal == 14);

	// updating a non existing item must work too
	hashTable.update(s2, 15);
	assert (hashTable.get(s2) == 15);
}


void SimpleHashTableTest::testOverflow()
{
	SimpleHashTable<std::string, int> hashTable(31);
	for (int i = 0; i < 31; ++i)
	{
		hashTable.insert(Poco::NumberFormatter::format(i), i*i);
	}

	for (int i = 0; i < 31; ++i)
	{
		std::string tmp = Poco::NumberFormatter::format(i);
		assert (hashTable.exists(tmp));
		assert (hashTable.get(tmp) == i*i);
	}
}


void SimpleHashTableTest::testSize()
{
	SimpleHashTable<std::string, int> hashTable(13);
	assert (hashTable.size() == 0);
	Poco::UInt32 POCO_UNUSED h1 = hashTable.insert("1", 1);
	assert (hashTable.size() == 1);
	Poco::UInt32 POCO_UNUSED h2 = hashTable.update("2", 2);
	assert (hashTable.size() == 2);
	hashTable.clear();
	assert (hashTable.size() == 0);
}


void SimpleHashTableTest::testResize()
{
	SimpleHashTable<std::string, int> hashTable(13);
	assert (hashTable.size() == 0);
	hashTable.resize(2467);
	for (int i = 0; i < 1024; ++i)
	{
		hashTable.insert(Poco::NumberFormatter::format(i), i*i);
	}
	hashTable.resize(3037);

	for (int i = 0; i < 1024; ++i)
	{
		std::string tmp = Poco::NumberFormatter::format(i);
		assert (hashTable.exists(tmp));
		assert (hashTable.get(tmp) == i*i);
	}
}


void SimpleHashTableTest::setUp()
{
}


void SimpleHashTableTest::tearDown()
{
}


CppUnit::Test* SimpleHashTableTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("SimpleHashTableTest");

	CppUnit_addTest(pSuite, SimpleHashTableTest, testInsert);
	CppUnit_addTest(pSuite, SimpleHashTableTest, testUpdate);
	CppUnit_addTest(pSuite, SimpleHashTableTest, testOverflow);
	CppUnit_addTest(pSuite, SimpleHashTableTest, testSize);
	CppUnit_addTest(pSuite, SimpleHashTableTest, testResize);

	return pSuite;
}
