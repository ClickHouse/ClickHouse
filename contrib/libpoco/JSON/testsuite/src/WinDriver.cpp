//
// WinDriver.cpp
//
// $Id: //poco/1.4/JSON/testsuite/src/WinDriver.cpp#1 $
//
// Windows test driver for Poco JSON.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "WinTestRunner/WinTestRunner.h"
#include "JSONTestSuite.h"


class TestDriver: public CppUnit::WinTestRunnerApp
{
	void TestMain()
	{
		CppUnit::WinTestRunner runner;
		runner.addTest(JSONTestSuite::suite());
		runner.run();
	}
};


TestDriver theDriver;
