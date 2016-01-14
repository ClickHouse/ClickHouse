//
// HTTPSServerTest.h
//
// $Id: //poco/1.4/NetSSL_OpenSSL/testsuite/src/HTTPSServerTest.h#1 $
//
// Definition of the HTTPSServerTest class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef HTTPSServerTest_INCLUDED
#define HTTPSServerTest_INCLUDED


#include "Poco/Net/Net.h"
#include "CppUnit/TestCase.h"


class HTTPSServerTest: public CppUnit::TestCase
{
public:
	HTTPSServerTest(const std::string& name);
	~HTTPSServerTest();

	void testIdentityRequest();
	void testChunkedRequest();
	void testIdentityRequestKeepAlive();
	void testChunkedRequestKeepAlive();
	void test100Continue();
	void testRedirect();
	void testAuth();
	void testNotImpl();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // HTTPSServerTest_INCLUDED
