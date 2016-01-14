//
// ProcessesTestSuite.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/ProcessesTestSuite.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "ProcessesTestSuite.h"
#include "ProcessTest.h"
#include "NamedMutexTest.h"
#include "NamedEventTest.h"
#include "SharedMemoryTest.h"


CppUnit::Test* ProcessesTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("ProcessesTestSuite");

	pSuite->addTest(ProcessTest::suite());
	pSuite->addTest(NamedMutexTest::suite());
	pSuite->addTest(NamedEventTest::suite());
	pSuite->addTest(SharedMemoryTest::suite());

	return pSuite;
}
