//
// HTTPServerTest.h
//
// $Id: //poco/1.4/Net/testsuite/src/HTTPServerTest.h#1 $
//
// Definition of the HTTPServerTest class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef HTTPServerTest_INCLUDED
#define HTTPServerTest_INCLUDED


#include "Poco/Net/Net.h"
#include "CppUnit/TestCase.h"


class HTTPServerTest: public CppUnit::TestCase
{
public:
	HTTPServerTest(const std::string& name);
	~HTTPServerTest();

	void testIdentityRequest();
	void testPutIdentityRequest();
	void testChunkedRequest();
	void testClosedRequest();
	void testIdentityRequestKeepAlive();
	void testChunkedRequestKeepAlive();
	void testClosedRequestKeepAlive();
	void testMaxKeepAlive();
	void testKeepAliveTimeout();
	void test100Continue();
	void testRedirect();
	void testAuth();
	void testNotImpl();
	void testBuffer();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // HTTPServerTest_INCLUDED
