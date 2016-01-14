//
// AutoPtrTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/AutoPtrTest.h#1 $
//
// Definition of the AutoPtrTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef AutoPtrTest_INCLUDED
#define AutoPtrTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class AutoPtrTest: public CppUnit::TestCase
{
public:
	AutoPtrTest(const std::string& name);
	~AutoPtrTest();

	void testAutoPtr();
	void testOps();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // AutoPtrTest_INCLUDED
