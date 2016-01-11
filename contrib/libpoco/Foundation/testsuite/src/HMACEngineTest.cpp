//
// HMACEngineTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/HMACEngineTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "HMACEngineTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/HMACEngine.h"
#include "Poco/MD5Engine.h"


using Poco::HMACEngine;
using Poco::MD5Engine;
using Poco::DigestEngine;


HMACEngineTest::HMACEngineTest(const std::string& name): CppUnit::TestCase(name)
{
}


HMACEngineTest::~HMACEngineTest()
{
}


void HMACEngineTest::testHMAC()
{
	// test vectors from RFC 2104
	
	std::string key(16, 0x0b);
	std::string data("Hi There");
	HMACEngine<MD5Engine> hmac1(key);
	hmac1.update(data);
	std::string digest = DigestEngine::digestToHex(hmac1.digest());
	assert (digest == "9294727a3638bb1c13f48ef8158bfc9d");
	
	key  = "Jefe";
	data = "what do ya want for nothing?";
	HMACEngine<MD5Engine> hmac2(key);
	hmac2.update(data);
	digest = DigestEngine::digestToHex(hmac2.digest());
	assert (digest == "750c783e6ab0b503eaa86e310a5db738");
	
	key  = std::string(16, 0xaa);
	data = std::string(50, 0xdd);
	HMACEngine<MD5Engine> hmac3(key);
	hmac3.update(data);
	digest = DigestEngine::digestToHex(hmac3.digest());
	assert (digest == "56be34521d144c88dbb8c733f0e8b3f6");
}


void HMACEngineTest::setUp()
{
}


void HMACEngineTest::tearDown()
{
}


CppUnit::Test* HMACEngineTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("HMACEngineTest");

	CppUnit_addTest(pSuite, HMACEngineTest, testHMAC);

	return pSuite;
}
