//
// uuidgen.cpp
//
// $Id: //poco/1.4/Foundation/samples/uuidgen/src/uuidgen.cpp#1 $
//
// This sample demonstrates the UUIDGenerator and UUID classes.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/UUID.h"
#include "Poco/UUIDGenerator.h"
#include "Poco/Exception.h"
#include <iostream>


using Poco::UUID;
using Poco::UUIDGenerator;
using Poco::Exception;


int main(int argc, char** argv)
{
	UUID uuid;
	
	std::string arg;
	if (argc > 1)
		arg = argv[1];
	
	try
	{
		if (arg == "-random")
			uuid = UUIDGenerator::defaultGenerator().createRandom();
		else if (arg.empty())
			uuid = UUIDGenerator::defaultGenerator().create();
		else
			uuid = UUIDGenerator::defaultGenerator().createFromName(UUID::uri(), arg);
		
		std::cout << uuid.toString() << std::endl;
	}
	catch (Exception& exc)
	{
		std::cerr << exc.displayText() << std::endl;
		return 1;
	}
	
	return 0;
}
