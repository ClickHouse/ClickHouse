//
// ODBCTestSuite.cpp
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "MySQLTestSuite.h"
#include "MySQLTest.h"

CppUnit::Test* MySQLTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("MySQLTestSuite");

	addTest(pSuite, MySQLTest::suite());
	return pSuite;
}


void MySQLTestSuite::addTest(CppUnit::TestSuite* pSuite, CppUnit::Test* pT)
{
	if (pSuite && pT) pSuite->addTest(pT);
}
