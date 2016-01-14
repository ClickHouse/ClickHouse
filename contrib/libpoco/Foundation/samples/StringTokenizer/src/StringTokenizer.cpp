//
// StringTokenizer.cpp
//
// $Id: //poco/1.4/Foundation/samples/StringTokenizer/src/StringTokenizer.cpp#1 $
//
// This sample demonstrates the usage of the StringTokenizer class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/StringTokenizer.h"
#include <iostream>


using Poco::StringTokenizer;


int main(int argc, char** argv)
{
	std::string tokens = "white; black; magenta, blue, green; yellow";
	StringTokenizer tokenizer(tokens, ";,", StringTokenizer::TOK_TRIM);
	for (StringTokenizer::Iterator it = tokenizer.begin(); it != tokenizer.end(); ++it)
	{
		std::cout << *it << std::endl;
	}
	return 0;
}
