//
// MD5EngineTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/MD5EngineTest.h#1 $
//
// Definition of the MD5EngineTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MD5EngineTest_INCLUDED
#define MD5EngineTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class MD5EngineTest: public CppUnit::TestCase
{
public:
	MD5EngineTest(const std::string& name);
	~MD5EngineTest();

	void testMD5();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // MD5EngineTest_INCLUDED
