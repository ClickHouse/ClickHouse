//
// HashingTestSuite.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/HashingTestSuite.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "HashingTestSuite.h"
#include "HashTableTest.h"
#include "SimpleHashTableTest.h"
#include "LinearHashTableTest.h"
#include "HashSetTest.h"
#include "HashMapTest.h"


CppUnit::Test* HashingTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("HashingTestSuite");

	pSuite->addTest(HashTableTest::suite());
	pSuite->addTest(SimpleHashTableTest::suite());
	pSuite->addTest(LinearHashTableTest::suite());
	pSuite->addTest(HashSetTest::suite());
	pSuite->addTest(HashMapTest::suite());

	return pSuite;
}
