//
// DataTestSuite.cpp
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DataTestSuite.h"
#include "DataTest.h"
#include "SessionPoolTest.h"


CppUnit::Test* DataTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("DataTestSuite");

	pSuite->addTest(DataTest::suite());
	pSuite->addTest(SessionPoolTest::suite());

	return pSuite;
}
