//
// WinDriver.cpp
//
// $Id: //poco/Main/Data/testsuite/src/WinDriver.cpp#1 $
//
// Windows test driver for Poco Data.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "WinTestRunner/WinTestRunner.h"
#include "DataTestSuite.h"


class TestDriver: public CppUnit::WinTestRunnerApp
{
	void TestMain()
	{
		CppUnit::WinTestRunner runner;
		runner.addTest(DataTestSuite::suite());
		runner.run();
	}
};


TestDriver theDriver;
