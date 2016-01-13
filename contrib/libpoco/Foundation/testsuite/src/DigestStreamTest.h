//
// DigestStreamTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/DigestStreamTest.h#1 $
//
// Definition of the DigestStreamTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DigestStreamTest_INCLUDED
#define DigestStreamTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class DigestStreamTest: public CppUnit::TestCase
{
public:
	DigestStreamTest(const std::string& name);
	~DigestStreamTest();

	void testInputStream();
	void testOutputStream1();
	void testOutputStream2();
	void testToFromHex();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // DigestStreamTest_INCLUDED
