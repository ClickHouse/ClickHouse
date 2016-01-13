//
// PBKDF2EngineTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/PBKDF2EngineTest.cpp#1 $
//
// Copyright (c) 2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "PBKDF2EngineTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/PBKDF2Engine.h"
#include "Poco/HMACEngine.h"
#include "Poco/SHA1Engine.h"


using Poco::PBKDF2Engine;
using Poco::HMACEngine;
using Poco::SHA1Engine;
using Poco::DigestEngine;


PBKDF2EngineTest::PBKDF2EngineTest(const std::string& name): CppUnit::TestCase(name)
{
}


PBKDF2EngineTest::~PBKDF2EngineTest()
{
}


void PBKDF2EngineTest::testPBKDF2a()
{
	// test vector 1 from RFC 6070
	
	std::string p("password");
	std::string s("salt");
	PBKDF2Engine<HMACEngine<SHA1Engine> > pbkdf2(s, 1, 20);
	pbkdf2.update(p);
	std::string dk = DigestEngine::digestToHex(pbkdf2.digest());
	assert (dk == "0c60c80f961f0e71f3a9b524af6012062fe037a6"); 
}


void PBKDF2EngineTest::testPBKDF2b()
{
	// test vector 2 from RFC 6070
	
	std::string p("password");
	std::string s("salt");
	PBKDF2Engine<HMACEngine<SHA1Engine> > pbkdf2(s, 2, 20);
	pbkdf2.update(p);
	std::string dk = DigestEngine::digestToHex(pbkdf2.digest());
	assert (dk == "ea6c014dc72d6f8ccd1ed92ace1d41f0d8de8957");
}


void PBKDF2EngineTest::testPBKDF2c()
{
	// test vector 3 from RFC 6070
	
	std::string p("password");
	std::string s("salt");
	PBKDF2Engine<HMACEngine<SHA1Engine> > pbkdf2(s, 4096, 20);
	pbkdf2.update(p);
	std::string dk = DigestEngine::digestToHex(pbkdf2.digest());
	assert (dk == "4b007901b765489abead49d926f721d065a429c1");
}


void PBKDF2EngineTest::testPBKDF2d()
{
	// test vector 4 from RFC 6070
	
	std::string p("password");
	std::string s("salt");
	PBKDF2Engine<HMACEngine<SHA1Engine> > pbkdf2(s, 16777216, 20);
	pbkdf2.update(p);
	std::string dk = DigestEngine::digestToHex(pbkdf2.digest());
	assert (dk == "eefe3d61cd4da4e4e9945b3d6ba2158c2634e984");
}


void PBKDF2EngineTest::testPBKDF2e()
{
	// test vector 5 from RFC 6070
	
	std::string p("passwordPASSWORDpassword");
	std::string s("saltSALTsaltSALTsaltSALTsaltSALTsalt");
	PBKDF2Engine<HMACEngine<SHA1Engine> > pbkdf2(s, 4096, 25);
	pbkdf2.update(p);
	std::string dk = DigestEngine::digestToHex(pbkdf2.digest());
	assert (dk == "3d2eec4fe41c849b80c8d83662c0e44a8b291a964cf2f07038");
}


void PBKDF2EngineTest::testPBKDF2f()
{
	// test vector 6 from RFC 6070
	
	std::string p("pass\0word", 9);
	std::string s("sa\0lt", 5);
	PBKDF2Engine<HMACEngine<SHA1Engine> > pbkdf2(s, 4096, 16);
	pbkdf2.update(p);
	std::string dk = DigestEngine::digestToHex(pbkdf2.digest());
	assert (dk == "56fa6aa75548099dcc37d7f03425e0c3");
}


void PBKDF2EngineTest::setUp()
{
}


void PBKDF2EngineTest::tearDown()
{
}


CppUnit::Test* PBKDF2EngineTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("PBKDF2EngineTest");

	CppUnit_addTest(pSuite, PBKDF2EngineTest, testPBKDF2a);
	CppUnit_addTest(pSuite, PBKDF2EngineTest, testPBKDF2b);
	CppUnit_addTest(pSuite, PBKDF2EngineTest, testPBKDF2c);
	CppUnit_addTest(pSuite, PBKDF2EngineTest, testPBKDF2d);
	CppUnit_addTest(pSuite, PBKDF2EngineTest, testPBKDF2e);
	CppUnit_addTest(pSuite, PBKDF2EngineTest, testPBKDF2f);

	return pSuite;
}
