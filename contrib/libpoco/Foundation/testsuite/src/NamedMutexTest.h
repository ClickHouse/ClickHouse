//
// NamedMutexTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/NamedMutexTest.h#1 $
//
// Definition of the NamedMutexTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NamedMutexTest_INCLUDED
#define NamedMutexTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class NamedMutexTest: public CppUnit::TestCase
{
public:
	NamedMutexTest(const std::string& name);
	~NamedMutexTest();

	void testLock();
	void testTryLock();
	
	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // NamedMutexTest_INCLUDED
