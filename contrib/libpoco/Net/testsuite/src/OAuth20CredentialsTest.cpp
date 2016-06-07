//
// OAuth20CredentialsTest.cpp
//
// $Id$
//
// Copyright (c) 2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "OAuth20CredentialsTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Net/HTTPRequest.h"
#include "Poco/Net/OAuth20Credentials.h"
#include "Poco/Net/NetException.h"


using Poco::Net::HTTPRequest;
using Poco::Net::OAuth20Credentials;
using Poco::Net::NotAuthenticatedException;


OAuth20CredentialsTest::OAuth20CredentialsTest(const std::string& name): CppUnit::TestCase(name)
{
}


OAuth20CredentialsTest::~OAuth20CredentialsTest()
{
}


void OAuth20CredentialsTest::testAuthorize()
{
	OAuth20Credentials creds("s3cr3tt0k3n");
	HTTPRequest request(HTTPRequest::HTTP_GET, "/");
	creds.authenticate(request);
	std::string auth = request.get("Authorization");	
	assert (auth == "Bearer s3cr3tt0k3n");
}


void OAuth20CredentialsTest::testAuthorizeCustomScheme()
{
	OAuth20Credentials creds("s3cr3tt0k3n", "token");
	HTTPRequest request(HTTPRequest::HTTP_GET, "/");
	creds.authenticate(request);
	std::string auth = request.get("Authorization");	
	assert (auth == "token s3cr3tt0k3n");
}


void OAuth20CredentialsTest::testExtract()
{
	HTTPRequest request(HTTPRequest::HTTP_GET, "/");
	request.set("Authorization", "Bearer s3cr3tt0k3n");
	OAuth20Credentials creds(request);
	assert (creds.getBearerToken() == "s3cr3tt0k3n");
}


void OAuth20CredentialsTest::testExtractCustomScheme()
{
	HTTPRequest request(HTTPRequest::HTTP_GET, "/");
	request.set("Authorization", "token s3cr3tt0k3n");
	OAuth20Credentials creds(request, "token");
	assert (creds.getBearerToken() == "s3cr3tt0k3n");
}


void OAuth20CredentialsTest::testExtractNoCreds()
{
	HTTPRequest request(HTTPRequest::HTTP_GET, "/");
	try
	{
		OAuth20Credentials creds(request);
		fail("no credentials - must throw");
	}
	catch (NotAuthenticatedException&)
	{
	}
}


void OAuth20CredentialsTest::setUp()
{
}


void OAuth20CredentialsTest::tearDown()
{
}


CppUnit::Test* OAuth20CredentialsTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("OAuth20CredentialsTest");

	CppUnit_addTest(pSuite, OAuth20CredentialsTest, testAuthorize);
	CppUnit_addTest(pSuite, OAuth20CredentialsTest, testAuthorizeCustomScheme);
	CppUnit_addTest(pSuite, OAuth20CredentialsTest, testExtract);
	CppUnit_addTest(pSuite, OAuth20CredentialsTest, testExtractCustomScheme);
	CppUnit_addTest(pSuite, OAuth20CredentialsTest, testExtractNoCreds);

	return pSuite;
}
