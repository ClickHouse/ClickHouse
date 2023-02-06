//
// ODBCTestSuite.h
//
// Definition of the ODBCTestSuite class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef ODBCTestSuite_INCLUDED
#define ODBCTestSuite_INCLUDED


#include "CppUnit/TestSuite.h"


class ODBCTestSuite
{
public:
	static CppUnit::Test* suite();

private:
	static void addTest(CppUnit::TestSuite* pSuite, CppUnit::Test* pT);
};


#endif // ODBCTestSuite_INCLUDED
