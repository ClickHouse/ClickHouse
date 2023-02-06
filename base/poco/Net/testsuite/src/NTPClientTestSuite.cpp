//
// NTPClientTestSuite.cpp
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "NTPClientTestSuite.h"
#include "NTPClientTest.h"


CppUnit::Test* NTPClientTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("NTPClientTestSuite");

	pSuite->addTest(NTPClientTest::suite());

	return pSuite;
}
