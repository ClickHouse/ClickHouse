//
// WinDriver.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/WinDriver.cpp#1 $
//
// Test driver for Windows.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "WinTestRunner/WinTestRunner.h"
#include "FoundationTestSuite.h"


class TestDriver: public CppUnit::WinTestRunnerApp
{
	void TestMain()
	{
		CppUnit::WinTestRunner runner;
		runner.addTest(FoundationTestSuite::suite());
		runner.run();
	}
};


TestDriver theDriver;
