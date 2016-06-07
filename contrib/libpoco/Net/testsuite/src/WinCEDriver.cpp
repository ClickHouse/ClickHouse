//
// WinCEDriver.cpp
//
// $Id: //poco/1.4/Net/testsuite/src/WinCEDriver.cpp#1 $
//
// Console-based test driver for Windows CE.
//
// Copyright (c) 2004-2010, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CppUnit/TestRunner.h"
#include "NetTestSuite.h"
#include <cstdlib>


int wmain(int argc, wchar_t* argv[])
{
	std::vector<std::string> args;
	for (int i = 0; i < argc; ++i)
	{
		char buffer[1024];
		std::wcstombs(buffer, argv[i], sizeof(buffer));
		args.push_back(std::string(buffer));
	}
	CppUnit::TestRunner runner;	
	runner.addTest("NetTestSuite", NetTestSuite::suite());
	return runner.run(args) ? 0 : 1;
}
