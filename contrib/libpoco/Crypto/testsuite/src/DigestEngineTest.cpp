//
// DigestEngineTest.cpp
//
// $Id: //poco/1.4/Crypto/testsuite/src/DigestEngineTest.cpp#1 $
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DigestEngineTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Crypto/DigestEngine.h"


using Poco::Crypto::DigestEngine;


DigestEngineTest::DigestEngineTest(const std::string& name): CppUnit::TestCase(name)
{
}


DigestEngineTest::~DigestEngineTest()
{
}


void DigestEngineTest::testMD5()
{
	DigestEngine engine("MD5");

	// test vectors from RFC 1321

	engine.update("");
	assert (DigestEngine::digestToHex(engine.digest()) == "d41d8cd98f00b204e9800998ecf8427e");

	engine.update("a");
	assert (DigestEngine::digestToHex(engine.digest()) == "0cc175b9c0f1b6a831c399e269772661");

	engine.update("abc");
	assert (DigestEngine::digestToHex(engine.digest()) == "900150983cd24fb0d6963f7d28e17f72");

	engine.update("message digest");
	assert (DigestEngine::digestToHex(engine.digest()) == "f96b697d7cb7938d525a2f31aaf161d0");

	engine.update("abcdefghijklmnopqrstuvwxyz");
	assert (DigestEngine::digestToHex(engine.digest()) == "c3fcd3d76192e4007dfb496cca67e13b");
	
	engine.update("ABCDEFGHIJKLMNOPQRSTUVWXYZ");
	engine.update("abcdefghijklmnopqrstuvwxyz0123456789");
	assert (DigestEngine::digestToHex(engine.digest()) == "d174ab98d277d9f5a5611c2c9f419d9f");

	engine.update("12345678901234567890123456789012345678901234567890123456789012345678901234567890");
	assert (DigestEngine::digestToHex(engine.digest()) == "57edf4a22be3c955ac49da2e2107b67a");
}

void DigestEngineTest::testSHA1()
{
	DigestEngine engine("SHA1");

	// test vectors from FIPS 180-1

	engine.update("abc");
	assert (DigestEngine::digestToHex(engine.digest()) == "a9993e364706816aba3e25717850c26c9cd0d89d");

	engine.update("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq");
	assert (DigestEngine::digestToHex(engine.digest()) == "84983e441c3bd26ebaae4aa1f95129e5e54670f1");

	for (int i = 0; i < 1000000; ++i)
		engine.update('a');
	assert (DigestEngine::digestToHex(engine.digest()) == "34aa973cd4c4daa4f61eeb2bdbad27316534016f");
}

void DigestEngineTest::setUp()
{
}


void DigestEngineTest::tearDown()
{
}


CppUnit::Test* DigestEngineTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("DigestEngineTest");

	CppUnit_addTest(pSuite, DigestEngineTest, testMD5);
	CppUnit_addTest(pSuite, DigestEngineTest, testSHA1);

	return pSuite;
}
