//
// PBKDF2EngineTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/PBKDF2EngineTest.h#1 $
//
// Definition of the PBKDF2EngineTest class.
//
// Copyright (c) 2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef PBKDF2EngineTest_INCLUDED
#define PBKDF2EngineTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class PBKDF2EngineTest: public CppUnit::TestCase
{
public:
	PBKDF2EngineTest(const std::string& name);
	~PBKDF2EngineTest();

	void testPBKDF2a();
	void testPBKDF2b();
	void testPBKDF2c();
	void testPBKDF2d();
	void testPBKDF2e();
	void testPBKDF2f();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // PBKDF2EngineTest_INCLUDED
