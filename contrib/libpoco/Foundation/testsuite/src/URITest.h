//
// URITest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/URITest.h#2 $
//
// Definition of the URITest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef URITest_INCLUDED
#define URITest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class URITest: public CppUnit::TestCase
{
public:
	URITest(const std::string& name);
	~URITest();

	void testConstruction();
	void testParse();
	void testToString();
	void testCompare();
	void testNormalize();
	void testResolve();
	void testSwap();
	void testEncodeDecode();
	void testOther();
	void testFromPath();
	void testQueryParameters();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // URITest_INCLUDED
