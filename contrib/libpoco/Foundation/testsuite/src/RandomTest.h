//
// RandomTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/RandomTest.h#1 $
//
// Definition of the RandomTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef RandomTest_INCLUDED
#define RandomTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class RandomTest: public CppUnit::TestCase
{
public:
	RandomTest(const std::string& name);
	~RandomTest();

	void testSequence1();
	void testSequence2();
	void testDistribution1();
	void testDistribution2();
	void testDistribution3();
	
	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // RandomTest_INCLUDED
