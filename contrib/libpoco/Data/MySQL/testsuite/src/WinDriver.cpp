//
// WinDriver.cpp
//
// $Id: //poco/1.4/Data/MySQL/testsuite/src/WinDriver.cpp#1 $
//
// Windows test driver for Poco MySQL.
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "WinTestRunner/WinTestRunner.h"
#include "MySQLTestSuite.h"


class TestDriver: public CppUnit::WinTestRunnerApp
{
	void TestMain()
	{
		CppUnit::WinTestRunner runner;
		runner.addTest(MySQLTestSuite::suite());
		runner.run();
	}
};


TestDriver theDriver;