//
// HTTPCookieTest.h
//
// $Id: //poco/1.4/Net/testsuite/src/HTTPCookieTest.h#2 $
//
// Definition of the HTTPCookieTest class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef HTTPCookieTest_INCLUDED
#define HTTPCookieTest_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/DateTime.h"
#include "CppUnit/TestCase.h"


class HTTPCookieTest: public CppUnit::TestCase
{
public:
	HTTPCookieTest(const std::string& name);
	~HTTPCookieTest();

	void testCookie();
	void testEscape();
	void testUnescape();
	void testExpiryFuture();
	void testExpiryPast();
	void testCookieExpiry(Poco::DateTime expiryTime);

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // HTTPCookieTest_INCLUDED
