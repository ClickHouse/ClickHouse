//
// HTTPCredentialsTest.h
//
// $Id: //poco/1.4/Net/testsuite/src/HTTPCredentialsTest.h#3 $
//
// Definition of the HTTPCredentialsTest class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef HTTPCredentialsTest_INCLUDED
#define HTTPCredentialsTest_INCLUDED


#include "Poco/Net/Net.h"
#include "CppUnit/TestCase.h"


class HTTPCredentialsTest: public CppUnit::TestCase
{
public:
	HTTPCredentialsTest(const std::string& name);
	~HTTPCredentialsTest();

	void testBasicCredentials();
	void testProxyBasicCredentials();
	void testBadCredentials();
	void testAuthenticationParams();
	void testAuthenticationParamsMultipleHeaders();
	void testDigestCredentials();
	void testDigestCredentialsQoP();
	void testCredentialsBasic();
	void testProxyCredentialsBasic();
	void testCredentialsDigest();
	void testCredentialsDigestMultipleHeaders();
	void testProxyCredentialsDigest();
	void testExtractCredentials();
	void testVerifyAuthInfo();
	void testVerifyAuthInfoQoP();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // HTTPCredentialsTest_INCLUDED
