//
// SharedPtrTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/SharedPtrTest.h#1 $
//
// Definition of the SharedPtrTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef SharedPtrTest_INCLUDED
#define SharedPtrTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class SharedPtrTest: public CppUnit::TestCase
{
public:
	SharedPtrTest(const std::string& name);
	~SharedPtrTest();

	void testSharedPtr();

	void testImplicitCast();
	void testExplicitCast();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // SharedPtrTest_INCLUDED
