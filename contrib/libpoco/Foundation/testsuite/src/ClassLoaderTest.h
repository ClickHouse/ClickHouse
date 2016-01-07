//
// ClassLoaderTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/ClassLoaderTest.h#1 $
//
// Definition of the ClassLoaderTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef ClassLoaderTest_INCLUDED
#define ClassLoaderTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class ClassLoaderTest: public CppUnit::TestCase
{
public:
	ClassLoaderTest(const std::string& name);
	~ClassLoaderTest();

	void testClassLoader1();
	void testClassLoader2();
	void testClassLoader3();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // ClassLoaderTest_INCLUDED
