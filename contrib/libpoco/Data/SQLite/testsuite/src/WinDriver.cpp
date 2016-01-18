//
// WinDriver.cpp
//
// $Id: //poco/Main/Data/SQLite/testsuite/src/WinDriver.cpp#2 $
//
// Windows test driver for Poco SQLite.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "WinTestRunner/WinTestRunner.h"
#include "SQLiteTestSuite.h"


class TestDriver: public CppUnit::WinTestRunnerApp
{
	void TestMain()
	{
		CppUnit::WinTestRunner runner;
		runner.addTest(SQLiteTestSuite::suite());
		runner.run();
	}
};


TestDriver theDriver;
