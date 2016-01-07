//
// ObjectPoolTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/ObjectPoolTest.cpp#1 $
//
// Copyright (c) 2010-2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "ObjectPoolTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/ObjectPool.h"
#include "Poco/Exception.h"


using Poco::ObjectPool;


ObjectPoolTest::ObjectPoolTest(const std::string& name): CppUnit::TestCase(name)
{
}


ObjectPoolTest::~ObjectPoolTest()
{
}


void ObjectPoolTest::testObjectPool()
{
	ObjectPool<std::string, Poco::SharedPtr<std::string> > pool(3, 4);
	
	assert (pool.capacity() == 3);
	assert (pool.peakCapacity() == 4);
	assert (pool.size() == 0);
	assert (pool.available() == 4);
	
	Poco::SharedPtr<std::string> pStr1 = pool.borrowObject();
	pStr1->assign("first");
	assert (pool.size() == 1);
	assert (pool.available() == 3);
	
	Poco::SharedPtr<std::string> pStr2 = pool.borrowObject();
	pStr2->assign("second");
	assert (pool.size() == 2);
	assert (pool.available() == 2);

	Poco::SharedPtr<std::string> pStr3 = pool.borrowObject();
	pStr3->assign("third");
	assert (pool.size() == 3);
	assert (pool.available() == 1);
	
	Poco::SharedPtr<std::string> pStr4 = pool.borrowObject();
	pStr4->assign("fourth");
	assert (pool.size() == 4);
	assert (pool.available() == 0);
	
	Poco::SharedPtr<std::string> pStr5 = pool.borrowObject();
	assert (pStr5.isNull());
	
	pool.returnObject(pStr4);
	assert (pool.size() == 4);
	assert (pool.available() == 1);
	
	pool.returnObject(pStr3);
	assert (pool.size() == 4);
	assert (pool.available() == 2);

	pStr3 = pool.borrowObject();
	assert (*pStr3 == "third");
	assert (pool.available() == 1);

	pool.returnObject(pStr3);
	pool.returnObject(pStr2);
	pool.returnObject(pStr1);
	
	assert (pool.size() == 3);
	assert (pool.available() == 4);
	
	pStr1 = pool.borrowObject();
	assert (*pStr1 == "second");
	assert (pool.available() == 3);

	pool.returnObject(pStr1);
	assert (pool.available() == 4);
}


void ObjectPoolTest::setUp()
{
}


void ObjectPoolTest::tearDown()
{
}


CppUnit::Test* ObjectPoolTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("ObjectPoolTest");

	CppUnit_addTest(pSuite, ObjectPoolTest, testObjectPool);

	return pSuite;
}
