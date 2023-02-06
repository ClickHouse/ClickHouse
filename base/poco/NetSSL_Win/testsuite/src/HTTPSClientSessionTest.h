//
// HTTPSClientSessionTest.h
//
// Definition of the HTTPSClientSessionTest class.
//
// Copyright (c) 2006-2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef HTTPSClientSessionTest_INCLUDED
#define HTTPSClientSessionTest_INCLUDED


#include "Poco/Net/Net.h"
#include "CppUnit/TestCase.h"


class HTTPSClientSessionTest: public CppUnit::TestCase
{
public:
	HTTPSClientSessionTest(const std::string& name);
	~HTTPSClientSessionTest();

	void testGetSmall();
	void testGetLarge();
	void testHead();
	void testPostSmallIdentity();
	void testPostLargeIdentity();
	void testPostSmallChunked();
	void testPostLargeChunked();
	void testPostLargeChunkedKeepAlive();
	void testKeepAlive();
	void testInterop();
	void testProxy();
	void testCachedSession();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // HTTPSClientSessionTest_INCLUDED
