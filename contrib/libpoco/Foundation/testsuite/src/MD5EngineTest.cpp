//
// MD5EngineTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/MD5EngineTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "MD5EngineTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/MD5Engine.h"


using Poco::MD5Engine;
using Poco::DigestEngine;


MD5EngineTest::MD5EngineTest(const std::string& name): CppUnit::TestCase(name)
{
}


MD5EngineTest::~MD5EngineTest()
{
}


void MD5EngineTest::testMD5()
{
	MD5Engine engine;

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


void MD5EngineTest::setUp()
{
}


void MD5EngineTest::tearDown()
{
}


CppUnit::Test* MD5EngineTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("MD5EngineTest");

	CppUnit_addTest(pSuite, MD5EngineTest, testMD5);

	return pSuite;
}
