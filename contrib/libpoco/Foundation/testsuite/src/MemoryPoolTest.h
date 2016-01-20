//
// MemoryPoolTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/MemoryPoolTest.h#1 $
//
// Definition of the MemoryPoolTest class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MemoryPoolTest_INCLUDED
#define MemoryPoolTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class MemoryPoolTest: public CppUnit::TestCase
{
public:
	MemoryPoolTest(const std::string& name);
	~MemoryPoolTest();

	void testMemoryPool();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // MemoryPoolTest_INCLUDED
