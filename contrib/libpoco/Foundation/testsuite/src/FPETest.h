//
// FPETest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/FPETest.h#1 $
//
// Definition of the FPETest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef FPETest_INCLUDED
#define FPETest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class FPETest: public CppUnit::TestCase
{
public:
	FPETest(const std::string& name);
	~FPETest();

	void testClassify();
	void testFlags();
	void testRound();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // FPETest_INCLUDED
