//
// DigestEngineTest.h
//
// $Id: //poco/1.4/Crypto/testsuite/src/DigestEngineTest.h#1 $
//
// Definition of the DigestEngineTest class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DigestEngineTest_INCLUDED
#define DigestEngineTest_INCLUDED


#include "Poco/Crypto/Crypto.h"
#include "CppUnit/TestCase.h"


class DigestEngineTest: public CppUnit::TestCase
{
public:
	DigestEngineTest(const std::string& name);
	~DigestEngineTest();

	void testMD5();
	void testSHA1();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // DigestEngineTest_INCLUDED
