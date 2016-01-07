//
// SHA1EngineTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/SHA1EngineTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "SHA1EngineTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/SHA1Engine.h"


using Poco::SHA1Engine;
using Poco::DigestEngine;


SHA1EngineTest::SHA1EngineTest(const std::string& name): CppUnit::TestCase(name)
{
}


SHA1EngineTest::~SHA1EngineTest()
{
}


void SHA1EngineTest::testSHA1()
{
	SHA1Engine engine;

	// test vectors from FIPS 180-1

	engine.update("abc");
	assert (DigestEngine::digestToHex(engine.digest()) == "a9993e364706816aba3e25717850c26c9cd0d89d");

	engine.update("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq");
	assert (DigestEngine::digestToHex(engine.digest()) == "84983e441c3bd26ebaae4aa1f95129e5e54670f1");

	for (int i = 0; i < 1000000; ++i)
		engine.update('a');
	assert (DigestEngine::digestToHex(engine.digest()) == "34aa973cd4c4daa4f61eeb2bdbad27316534016f");
}


void SHA1EngineTest::setUp()
{
}


void SHA1EngineTest::tearDown()
{
}


CppUnit::Test* SHA1EngineTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("SHA1EngineTest");

	CppUnit_addTest(pSuite, SHA1EngineTest, testSHA1);

	return pSuite;
}
