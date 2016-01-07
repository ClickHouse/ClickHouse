//
// NDCTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/NDCTest.h#1 $
//
// Definition of the NDCTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NDCTest_INCLUDED
#define NDCTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class NDCTest: public CppUnit::TestCase
{
public:
	NDCTest(const std::string& name);
	~NDCTest();

	void testNDC();
	void testNDCScope();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // NDCTest_INCLUDED
