//
// TypeListTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/TypeListTest.h#1 $
//
// Definition of the TypeListTest class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef TypeListTest_INCLUDED
#define TypeListTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class TypeListTest: public CppUnit::TestCase
{
public:
	TypeListTest(const std::string& name);
	~TypeListTest();

	void testTypeList();
	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // TypeListTest_INCLUDED
