//
// UUIDGeneratorTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/UUIDGeneratorTest.h#1 $
//
// Definition of the UUIDGeneratorTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef UUIDGeneratorTest_INCLUDED
#define UUIDGeneratorTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class UUIDGeneratorTest: public CppUnit::TestCase
{
public:
	UUIDGeneratorTest(const std::string& name);
	~UUIDGeneratorTest();

	void testTimeBased();
	void testRandom();
	void testNameBased();
	
	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // UUIDGeneratorTest_INCLUDED
