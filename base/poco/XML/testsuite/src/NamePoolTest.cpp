//
// NamePoolTest.cpp
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "NamePoolTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/XML/NamePool.h"
#include "Poco/XML/Name.h"
#include "Poco/DOM/AutoPtr.h"


using Poco::XML::NamePool;
using Poco::XML::Name;
using Poco::XML::AutoPtr;


NamePoolTest::NamePoolTest(const std::string& name): CppUnit::TestCase(name)
{
}


NamePoolTest::~NamePoolTest()
{
}


void NamePoolTest::testNamePool()
{
	AutoPtr<NamePool> pool = new NamePool;
	const Name* pName = 0;
	Name name("pre:local", "http://www.appinf.com");
	
	pName = &pool->insert(name);
	const Name* pName2 = &pool->insert("pre:local", "http://www.appinf.com", "local");
	assertTrue (pName == pName2);
	
	pName2 = &pool->insert("pre:local2", "http://www.appinf.com", "local2");
	assertTrue (pName2 != pName);
	
	pName2 = &pool->insert(name);
	assertTrue (pName2 == pName);
	
	pName2 = &pool->insert(*pName);
	assertTrue (pName2 == pName);
}


void NamePoolTest::setUp()
{
}


void NamePoolTest::tearDown()
{
}


CppUnit::Test* NamePoolTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("NamePoolTest");

	CppUnit_addTest(pSuite, NamePoolTest, testNamePool);

	return pSuite;
}
