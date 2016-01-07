//
// grep.cpp
//
// $Id: //poco/1.4/Foundation/samples/grep/src/grep.cpp#1 $
//
// This sample demonstrates the RegularExpression class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/RegularExpression.h"
#include <iostream>


using Poco::RegularExpression;


int main(int argc, char** argv)
{
	if (argc < 2)
	{
		std::cout << "usage: " << argv[0] << ": [-i] [-x] pattern" << std::endl;
		return 1;
	}
	
	std::string pattern;
	int options = 0;
	for (int i = 1; i < argc; ++i)
	{
		std::string arg(argv[i]);
		if (arg == "-i")
			options += RegularExpression::RE_CASELESS;
		else if (arg == "-x")
			options += RegularExpression::RE_EXTENDED;
		else
			pattern = arg;
	}
	
	RegularExpression re(pattern, options);
	
	int c = std::cin.get();
	while (c != -1)
	{
		std::string line;
		while (c != -1 && c != '\n')
		{
			line += (char) c;
			c = std::cin.get();
		}

		RegularExpression::Match mtch;
		if (re.match(line, mtch))
			std::cout << line << std::endl;

		if (c != -1) c = std::cin.get();
	}
	
	return 0;
}
