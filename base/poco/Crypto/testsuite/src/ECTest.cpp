//
// ECTest.cpp
//
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "ECTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Crypto/ECKey.h"
#include "Poco/Crypto/ECDSADigestEngine.h"
#include <openssl/pem.h>
#include <iostream>
#include <sstream>
#include <cstring>


using namespace Poco::Crypto;


ECTest::ECTest(const std::string& name): CppUnit::TestCase(name)
{
}


ECTest::~ECTest()
{
}


void ECTest::testECNewKeys()
{
	try
	{
		std::string curveName = ECKey::getCurveName();
		if (!curveName.empty())
		{
			ECKey key(curveName);
			std::ostringstream strPub;
			std::ostringstream strPriv;
			key.save(&strPub, &strPriv, "testpwd");
			std::string pubKey = strPub.str();
			std::string privKey = strPriv.str();

			// now do the round trip
			std::istringstream iPub(pubKey);
			std::istringstream iPriv(privKey);
			ECKey key2(&iPub, &iPriv, "testpwd");

			std::istringstream iPriv2(privKey);
			ECKey key3(0, &iPriv2, "testpwd");
			std::ostringstream strPub3;
			key3.save(&strPub3);
			std::string pubFromPrivate = strPub3.str();
			assert (pubFromPrivate == pubKey);
		}
		else
			std::cerr << "No elliptic curves found!" << std::endl;
	}
	catch (Poco::Exception& ex)
	{
		std::cerr << ex.displayText() << std::endl;
		throw;
	}
}


void ECTest::testECNewKeysNoPassphrase()
{
	try
	{
		std::string curveName = ECKey::getCurveName();
		if (!curveName.empty())
		{
			ECKey key(curveName);
			std::ostringstream strPub;
			std::ostringstream strPriv;
			key.save(&strPub, &strPriv);
			std::string pubKey = strPub.str();
			std::string privKey = strPriv.str();

			// now do the round trip
			std::istringstream iPub(pubKey);
			std::istringstream iPriv(privKey);
			ECKey key2(&iPub, &iPriv);

			std::istringstream iPriv2(privKey);
			ECKey key3(0, &iPriv2);
			std::ostringstream strPub3;
			key3.save(&strPub3);
			std::string pubFromPrivate = strPub3.str();
			assert (pubFromPrivate == pubKey);
		}
		else
			std::cerr << "No elliptic curves found!" << std::endl;
	}
	catch (Poco::Exception& ex)
	{
		std::cerr << ex.displayText() << std::endl;
		throw;
	}
}


void ECTest::testECDSASignSha256()
{
	try
	{
		std::string curveName = ECKey::getCurveName();
		if (!curveName.empty())
		{
			std::string msg("Test this sign message");
			ECKey key(curveName);
			ECDSADigestEngine eng(key, "SHA256");
			eng.update(msg.c_str(), static_cast<unsigned>(msg.length()));
			const Poco::DigestEngine::Digest& sig = eng.signature();

			// verify
			std::ostringstream strPub;
			key.save(&strPub);
			std::string pubKey = strPub.str();
			std::istringstream iPub(pubKey);
			ECKey keyPub(&iPub);
			ECDSADigestEngine eng2(keyPub, "SHA256");
			eng2.update(msg.c_str(), static_cast<unsigned>(msg.length()));
			assert(eng2.verify(sig));
		}
		else
			std::cerr << "No elliptic curves found!" << std::endl;
	}
	catch (Poco::Exception& ex)
	{
		std::cerr << ex.displayText() << std::endl;
		throw;
	}
}


void ECTest::testECDSASignManipulated()
{
	try
	{
		std::string curveName = ECKey::getCurveName();
		if (!curveName.empty())
		{
			std::string msg("Test this sign message");
			std::string msgManip("Test that sign message");
			ECKey key(curveName);
			ECDSADigestEngine eng(key, "SHA256");
			eng.update(msg.c_str(), static_cast<unsigned>(msg.length()));
			const Poco::DigestEngine::Digest& sig = eng.signature();
			std::string hexDig = Poco::DigestEngine::digestToHex(sig);

			// verify
			std::ostringstream strPub;
			key.save(&strPub);
			std::string pubKey = strPub.str();
			std::istringstream iPub(pubKey);
			ECKey keyPub(&iPub);
			ECDSADigestEngine eng2(keyPub, "SHA256");
			eng2.update(msgManip.c_str(), static_cast<unsigned>(msgManip.length()));
			assert (!eng2.verify(sig));
		}
		else
			std::cerr << "No elliptic curves found!" << std::endl;
	}
	catch (Poco::Exception& ex)
	{
		std::cerr << ex.displayText() << std::endl;
		throw;
	}
}


void ECTest::setUp()
{
}


void ECTest::tearDown()
{
}


CppUnit::Test* ECTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("ECTest");

	CppUnit_addTest(pSuite, ECTest, testECNewKeys);
	CppUnit_addTest(pSuite, ECTest, testECNewKeysNoPassphrase);
	CppUnit_addTest(pSuite, ECTest, testECDSASignSha256);
	CppUnit_addTest(pSuite, ECTest, testECDSASignManipulated);

	return pSuite;
}
