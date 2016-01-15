//
// base64encode.cpp
//
// $Id: //poco/1.4/Foundation/samples/base64encode/src/base64encode.cpp#1 $
//
// This sample demonstrates the Base64Encoder and StreamCopier classes.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Base64Encoder.h"
#include "Poco/StreamCopier.h"
#include <iostream>
#include <fstream>


using Poco::Base64Encoder;
using Poco::StreamCopier;


int main(int argc, char** argv)
{
	if (argc != 3)
	{
		std::cout << "usage: " << argv[0] << ": <input_file> <output_file>" << std::endl
		          << "       read <input_file>, base64-encode it and write the result to <output_file>" << std::endl;
		return 1;
	}
	
	std::ifstream istr(argv[1], std::ios::binary);
	if (!istr)
	{
		std::cerr << "cannot open input file: " << argv[1] << std::endl;
		return 2;
	}
	
	std::ofstream ostr(argv[2]);
	if (!ostr)
	{
		std::cerr << "cannot open output file: " << argv[2] << std::endl;
		return 3;
	}
	
	Base64Encoder encoder(ostr);
	StreamCopier::copyStream(istr, encoder);
	
	if (!ostr)
	{
		std::cerr << "error writing output file: " << argv[2] << std::endl;
		return 4;
	}
	
	return 0;
}
