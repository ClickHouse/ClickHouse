//
// TaskTestSuite.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/TaskTestSuite.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "TaskTestSuite.h"
#include "TaskTest.h"
#include "TaskManagerTest.h"


CppUnit::Test* TaskTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("TaskTestSuite");

	pSuite->addTest(TaskTest::suite());
	pSuite->addTest(TaskManagerTest::suite());

	return pSuite;
}
