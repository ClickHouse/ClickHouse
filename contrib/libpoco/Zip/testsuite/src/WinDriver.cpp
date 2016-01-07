//
// WinDriver.cpp
//
// $Id: //poco/1.4/Zip/testsuite/src/WinDriver.cpp#1 $
//
// Windows test driver for Poco Zip.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "WinTestRunner/WinTestRunner.h"
#include "ZipTestSuite.h"


class TestDriver: public CppUnit::WinTestRunnerApp
{
	void TestMain()
	{
		CppUnit::WinTestRunner runner;
		runner.addTest(ZipTestSuite::suite());
		runner.run();
	}
};


TestDriver theDriver;
