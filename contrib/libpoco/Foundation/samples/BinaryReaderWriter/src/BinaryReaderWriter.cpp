//
// BinaryReaderWriter.cpp
//
// $Id: //poco/1.4/Foundation/samples/BinaryReaderWriter/src/BinaryReaderWriter.cpp#1 $
//
// This sample demonstrates the BinaryWriter and BinaryReader classes.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/BinaryWriter.h"
#include "Poco/BinaryReader.h"
#include <sstream>
#include <iostream>


using Poco::BinaryWriter;
using Poco::BinaryReader;


int main(int argc, char** argv)
{
	std::stringstream str;
	
	BinaryWriter writer(str);
	writer << true
	       << 'x'
	       << 42
	       << 3.14159265
	       << "foo bar";
	       
	bool   b;
	char   c;
	int    i;
	double d;
	std::string s;
	
	BinaryReader reader(str);
	reader >> b
	       >> c
	       >> i
	       >> d
	       >> s;
	       
	std::cout << b << std::endl
	          << c << std::endl
	          << i << std::endl
	          << d << std::endl
	          << s << std::endl;
	          	
	return 0;
}
