//
// WinCEDriver.cpp
//
// Console-based test driver for Windows CE.
//
// Copyright (c) 2004-2010, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CppUnit/TestRunner.h"
#include "XMLTestSuite.h"
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
	runner.addTest("XMLTestSuite", XMLTestSuite::suite());
	return runner.run(args) ? 0 : 1;
}
