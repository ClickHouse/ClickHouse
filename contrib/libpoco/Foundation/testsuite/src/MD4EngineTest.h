//
// MD4EngineTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/MD4EngineTest.h#1 $
//
// Definition of the MD4EngineTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MD4EngineTest_INCLUDED
#define MD4EngineTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class MD4EngineTest: public CppUnit::TestCase
{
public:
	MD4EngineTest(const std::string& name);
	~MD4EngineTest();

	void testMD4();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // MD4EngineTest_INCLUDED
