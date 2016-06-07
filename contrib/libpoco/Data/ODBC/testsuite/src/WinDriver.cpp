//
// WinDriver.cpp
//
// $Id: //poco/Main/Data/ODBC/testsuite/src/WinDriver.cpp#2 $
//
// Windows test driver for Poco ODBC.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "WinTestRunner/WinTestRunner.h"
#include "ODBCTestSuite.h"


class TestDriver: public CppUnit::WinTestRunnerApp
{
	void TestMain()
	{
		CppUnit::WinTestRunner runner;
		runner.addTest(ODBCTestSuite::suite());
		runner.run();
	}
};


TestDriver theDriver;
