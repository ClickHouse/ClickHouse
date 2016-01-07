//
// WinDriver.cpp
//
// $Id: //poco/1.4/Util/testsuite/src/WinDriver.cpp#1 $
//
// Windows test driver for Poco Util.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "WinTestRunner/WinTestRunner.h"
#include "UtilTestSuite.h"


class TestDriver: public CppUnit::WinTestRunnerApp
{
	void TestMain()
	{
		CppUnit::WinTestRunner runner;
		runner.addTest(UtilTestSuite::suite());
		runner.run();
	}
};


TestDriver theDriver;
