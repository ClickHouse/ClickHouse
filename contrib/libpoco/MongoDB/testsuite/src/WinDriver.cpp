//
// WinDriver.cpp
//
// $Id$
//
// Windows test driver for Poco MongoDB.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "WinTestRunner/WinTestRunner.h"
#include "MongoDBTestSuite.h"


class TestDriver: public CppUnit::WinTestRunnerApp
{
	void TestMain()
	{
		CppUnit::WinTestRunner runner;
		runner.addTest(MongoDBTestSuite::suite());
		runner.run();
	}
};


TestDriver theDriver;
