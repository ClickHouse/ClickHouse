//
// ODBCTestSuite.h
//
// $Id: //poco/1.4/Data/MySQL/testsuite/src/MySQLTestSuite.h#1 $
//
// Definition of the ODBCTestSuite class.
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MySQLTestSuite_INCLUDED
#define MySQLTestSuite_INCLUDED


#include "CppUnit/TestSuite.h"

class MySQLTestSuite
{
public:
	static CppUnit::Test* suite();

private:
	static void addTest(CppUnit::TestSuite* pSuite, CppUnit::Test* pT);
};


#endif // MySQLTestSuite_INCLUDED
