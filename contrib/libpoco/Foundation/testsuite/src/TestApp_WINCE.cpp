//
// TestApp_WINCE.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/TestApp_WINCE.cpp#1 $
//
// Copyright (c) 2005-2010, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include <string>
#include <iostream>


int wmain(int argc, wchar_t* argv[])
{
	if (argc > 1)
	{
		std::wstring arg(argv[1]);
		if (arg == L"-hello")
		{
			std::cout << "Hello, world!";
		}
		else if (arg == L"-count")
		{
			int n = 0;
			int c = std::cin.get();
			while (c != -1) { ++n; c = std::cin.get(); }
			return n;
		}
	}
	return argc - 1;
}
