//
// ZLibTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/ZLibTest.h#1 $
//
// Definition of the ZLibTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef ZLibTest_INCLUDED
#define ZLibTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class ZLibTest: public CppUnit::TestCase
{
public:
	ZLibTest(const std::string& name);
	~ZLibTest();

	void testDeflate1();
	void testDeflate2();
	void testDeflate3();
	void testDeflate4();
	void testGzip1();
	void testGzip2();
	void testGzip3();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // ZLibTest_INCLUDED
