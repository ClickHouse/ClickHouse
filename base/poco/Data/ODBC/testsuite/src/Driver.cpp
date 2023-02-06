//
// Driver.cpp
//
// Console-based test driver for Poco SQLite.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CppUnit/TestRunner.h"
#include "ODBCTestSuite.h"
#include "Poco/Data/ODBC/Connector.h"


int main(int ac, char **av)
{
	Poco::Data::ODBC::Connector::registerConnector();

	std::vector<std::string> args;
	for (int i = 0; i < ac; ++i)
		args.push_back(std::string(av[i]));
	CppUnit::TestRunner runner;
	runner.addTest("ODBCTestSuite", ODBCTestSuite::suite());
	return runner.run(args) ? 0 : 1;
}
