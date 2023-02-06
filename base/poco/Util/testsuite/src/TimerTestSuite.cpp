//
// TimerTestSuite.cpp
//
// Copyright (c) 2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "TimerTestSuite.h"
#include "TimerTest.h"


CppUnit::Test* TimerTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("TimerTestSuite");

	pSuite->addTest(TimerTest::suite());

	return pSuite;
}
