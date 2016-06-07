//
// SHA1EngineTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/SHA1EngineTest.h#1 $
//
// Definition of the SHA1EngineTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef SHA1EngineTest_INCLUDED
#define SHA1EngineTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class SHA1EngineTest: public CppUnit::TestCase
{
public:
	SHA1EngineTest(const std::string& name);
	~SHA1EngineTest();

	void testSHA1();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // SHA1EngineTest_INCLUDED
