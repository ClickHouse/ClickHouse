//
// ThreadTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/ThreadTest.h#1 $
//
// Definition of the ThreadTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef ThreadTest_INCLUDED
#define ThreadTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class ThreadTest: public CppUnit::TestCase
{
public:
	ThreadTest(const std::string& name);
	~ThreadTest();

	void testThread();
	void testNamedThread();
	void testCurrent();
	void testThreads();
	void testJoin();
	void testNotJoin();
	void testNotRun();
	void testNotRunJoin();
	void testTrySleep();
	void testThreadTarget();
	void testThreadFunction();
	void testThreadFunctor();
	void testThreadStackSize();
	void testSleep();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // ThreadTest_INCLUDED
