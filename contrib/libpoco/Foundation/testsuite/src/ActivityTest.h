//
// ActivityTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/ActivityTest.h#1 $
//
// Definition of the ActivityTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef ActivityTest_INCLUDED
#define ActivityTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class ActivityTest: public CppUnit::TestCase
{
public:
	ActivityTest(const std::string& name);
	~ActivityTest();

	void testActivity();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // ActivityTest_INCLUDED
