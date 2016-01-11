//
// deflate.cpp
//
// $Id: //poco/1.4/Foundation/samples/deflate/src/deflate.cpp#1 $
//
// This sample demonstrates the DeflatingOutputStream and StreamCopier classes.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/DeflatingStream.h"
#include "Poco/StreamCopier.h"
#include <iostream>
#include <fstream>


using Poco::DeflatingOutputStream;
using Poco::StreamCopier;


int main(int argc, char** argv)
{
	if (argc != 3)
	{
		std::cout << "usage: " << argv[0] << ": <input_file> <output_file>" << std::endl
		          << "       read <input_file>, deflate (compress) it and write the result to <output_file>" << std::endl;
		return 1;
	}
	
	std::ifstream istr(argv[1], std::ios::binary);
	if (!istr)
	{
		std::cerr << "cannot open input file: " << argv[1] << std::endl;
		return 2;
	}
	
	std::ofstream ostr(argv[2], std::ios::binary);
	if (!ostr)
	{
		std::cerr << "cannot open output file: " << argv[2] << std::endl;
		return 3;
	}
	
	DeflatingOutputStream deflater(ostr);
	StreamCopier::copyStream(istr, deflater);
	
	if (!ostr)
	{
		std::cerr << "error writing output file: " << argv[2] << std::endl;
		return 4;
	}
	
	return 0;
}
