//
// SQLiteTestSuite.cpp
//
// $Id: //poco/Main/Data/SQLite/testsuite/src/SQLiteTestSuite.cpp#2 $
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "SQLiteTestSuite.h"
#include "SQLiteTest.h"


CppUnit::Test* SQLiteTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("SQLiteTestSuite");

	pSuite->addTest(SQLiteTest::suite());

	return pSuite;
}
