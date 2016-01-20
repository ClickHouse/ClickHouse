//
// UUIDTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/UUIDTest.h#1 $
//
// Definition of the UUIDTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef UUIDTest_INCLUDED
#define UUIDTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class UUIDTest: public CppUnit::TestCase
{
public:
	UUIDTest(const std::string& name);
	~UUIDTest();

	void testParse();
	void testBuffer();
	void testCompare();
	void testVersion();
	void testVariant();
	void testSwap();
	void testTryParse();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // UUIDTest_INCLUDED
