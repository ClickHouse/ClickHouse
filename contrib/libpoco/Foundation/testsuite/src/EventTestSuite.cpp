//
// EventTestSuite.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/EventTestSuite.cpp#1 $
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "EventTestSuite.h"
#include "FIFOEventTest.h"
#include "BasicEventTest.h"
#include "PriorityEventTest.h"

CppUnit::Test* EventTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("EventTestSuite");

	pSuite->addTest(BasicEventTest::suite());
	pSuite->addTest(PriorityEventTest::suite());
	pSuite->addTest(FIFOEventTest::suite());

	return pSuite;
}
