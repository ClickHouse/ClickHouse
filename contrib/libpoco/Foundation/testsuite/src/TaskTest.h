//
// TaskTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/TaskTest.h#1 $
//
// Definition of the TaskTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef TaskTest_INCLUDED
#define TaskTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class TaskTest: public CppUnit::TestCase
{
public:
	TaskTest(const std::string& name);
	~TaskTest();

	void testFinish();
	void testCancel1();
	void testCancel2();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // TaskTest_INCLUDED
