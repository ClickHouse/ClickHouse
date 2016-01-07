//
// SQLiteTestSuite.h
//
// $Id: //poco/Main/Data/SQLite/testsuite/src/SQLiteTestSuite.h#2 $
//
// Definition of the SQLiteTestSuite class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef SQLiteTestSuite_INCLUDED
#define SQLiteTestSuite_INCLUDED


#include "CppUnit/TestSuite.h"


class SQLiteTestSuite
{
public:
	static CppUnit::Test* suite();
};


#endif // SQLiteTestSuite_INCLUDED
