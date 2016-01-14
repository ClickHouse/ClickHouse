//
// HTTPResponseTest.h
//
// $Id: //poco/1.4/Net/testsuite/src/HTTPResponseTest.h#1 $
//
// Definition of the HTTPResponseTest class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef HTTPResponseTest_INCLUDED
#define HTTPResponseTest_INCLUDED


#include "Poco/Net/Net.h"
#include "CppUnit/TestCase.h"


class HTTPResponseTest: public CppUnit::TestCase
{
public:
	HTTPResponseTest(const std::string& name);
	~HTTPResponseTest();

	void testWrite1();
	void testWrite2();
	void testRead1();
	void testRead2();
	void testRead3();
	void testInvalid1();
	void testInvalid2();
	void testInvalid3();
	void testCookies();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // HTTPResponseTest_INCLUDED
