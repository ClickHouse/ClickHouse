//
// RWLockTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/RWLockTest.h#1 $
//
// Definition of the RWLockTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef RWLockTest_INCLUDED
#define RWLockTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class RWLockTest: public CppUnit::TestCase
{
public:
	RWLockTest(const std::string& name);
	~RWLockTest();

	void testLock();
	void testTryLock();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // RWLockTest_INCLUDED
