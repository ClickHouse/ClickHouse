//
// DynamicFactoryTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/DynamicFactoryTest.h#1 $
//
// Definition of the DynamicFactoryTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DynamicFactoryTest_INCLUDED
#define DynamicFactoryTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class DynamicFactoryTest: public CppUnit::TestCase
{
public:
	DynamicFactoryTest(const std::string& name);
	~DynamicFactoryTest();

	void testDynamicFactory();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // DynamicFactoryTest_INCLUDED
