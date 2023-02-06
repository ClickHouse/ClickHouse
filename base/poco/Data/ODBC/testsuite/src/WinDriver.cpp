//
// WinDriver.cpp
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
#include "Poco/Data/ODBC/Connector.h"


class TestDriver: public CppUnit::WinTestRunnerApp
{
	void TestMain()
	{
		Poco::Data::ODBC::Connector::registerConnector();

		CppUnit::WinTestRunner runner;
		runner.addTest(ODBCTestSuite::suite());
		runner.run();
	}
};


TestDriver theDriver;
