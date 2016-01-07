//
// Driver.cpp
//
// $Id: //poco/1.4/Crypto/testsuite/src/Driver.cpp#1 $
//
// Console-based test driver for Poco Crypto.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CppUnit/TestRunner.h"
#include "CryptoTestSuite.h"
#include "Poco/Crypto/Crypto.h"


class CryptoInitializer
{
public:
	CryptoInitializer()
	{
		Poco::Crypto::initializeCrypto();
	}
	
	~CryptoInitializer()
	{
		Poco::Crypto::uninitializeCrypto();
	}
};


int main(int ac, char **av)
{
	CryptoInitializer ci;
	
	std::vector<std::string> args;
	for (int i = 0; i < ac; ++i)
		args.push_back(std::string(av[i]));
	CppUnit::TestRunner runner;
	runner.addTest("CryptoTestSuite", CryptoTestSuite::suite());
	return runner.run(args) ? 0 : 1;
}
