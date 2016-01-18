//
// SemaphoreTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/SemaphoreTest.h#1 $
//
// Definition of the SemaphoreTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef SemaphoreTest_INCLUDED
#define SemaphoreTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class SemaphoreTest: public CppUnit::TestCase
{
public:
	SemaphoreTest(const std::string& name);
	~SemaphoreTest();

	void testInitZero();
	void testInitNonZero();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // SemaphoreTest_INCLUDED
