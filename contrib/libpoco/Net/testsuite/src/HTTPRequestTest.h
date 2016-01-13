//
// HTTPRequestTest.h
//
// $Id: //poco/1.4/Net/testsuite/src/HTTPRequestTest.h#3 $
//
// Definition of the HTTPRequestTest class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef HTTPRequestTest_INCLUDED
#define HTTPRequestTest_INCLUDED


#include "Poco/Net/Net.h"
#include "CppUnit/TestCase.h"


class HTTPRequestTest: public CppUnit::TestCase
{
public:
	HTTPRequestTest(const std::string& name);
	~HTTPRequestTest();

	void testWrite1();
	void testWrite2();
	void testWrite3();
	void testWrite4();
	void testRead1();
	void testRead2();
	void testRead3();
	void testRead4();
	void testInvalid1();
	void testInvalid2();
	void testInvalid3();
	void testCookies();
	
	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // HTTPRequestTest_INCLUDED
