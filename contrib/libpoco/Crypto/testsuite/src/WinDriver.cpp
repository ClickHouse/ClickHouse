//
// WinDriver.cpp
//
// $Id: //poco/1.4/Crypto/testsuite/src/WinDriver.cpp#1 $
//
// Windows test driver for Poco Crypto.
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "WinTestRunner/WinTestRunner.h"
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


class TestDriver: public CppUnit::WinTestRunnerApp
{
	void TestMain()
	{
		CryptoInitializer ci;

		CppUnit::WinTestRunner runner;
		runner.addTest(CryptoTestSuite::suite());
		runner.run();
	}
};


TestDriver theDriver;
