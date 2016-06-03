//
// HTTPCredentialsTest.cpp
//
// $Id: //poco/1.4/Net/testsuite/src/HTTPCredentialsTest.cpp#3 $
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "HTTPCredentialsTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Net/HTTPRequest.h"
#include "Poco/Net/HTTPResponse.h"
#include "Poco/Net/HTTPBasicCredentials.h"
#include "Poco/Net/HTTPAuthenticationParams.h"
#include "Poco/Net/HTTPDigestCredentials.h"
#include "Poco/Net/HTTPCredentials.h"
#include "Poco/Net/NetException.h"
#include "Poco/URI.h"


using Poco::Net::HTTPRequest;
using Poco::Net::HTTPResponse;
using Poco::Net::HTTPBasicCredentials;
using Poco::Net::HTTPAuthenticationParams;
using Poco::Net::HTTPDigestCredentials;
using Poco::Net::HTTPCredentials;
using Poco::Net::NotAuthenticatedException;


HTTPCredentialsTest::HTTPCredentialsTest(const std::string& name): CppUnit::TestCase(name)
{
}


HTTPCredentialsTest::~HTTPCredentialsTest()
{
}


void HTTPCredentialsTest::testBasicCredentials()
{
	HTTPRequest request;
	assert (!request.hasCredentials());
	
	HTTPBasicCredentials cred("user", "secret");
	cred.authenticate(request);
	assert (request.hasCredentials());
	std::string scheme;
	std::string info;
	request.getCredentials(scheme, info);
	assert (scheme == "Basic");
	assert (info == "dXNlcjpzZWNyZXQ=");
	
	HTTPBasicCredentials cred2(request);
	assert (cred2.getUsername() == "user");
	assert (cred2.getPassword() == "secret");
}


void HTTPCredentialsTest::testProxyBasicCredentials()
{
	HTTPRequest request;
	assert (!request.hasProxyCredentials());
	
	HTTPBasicCredentials cred("user", "secret");
	cred.proxyAuthenticate(request);
	assert (request.hasProxyCredentials());
	std::string scheme;
	std::string info;
	request.getProxyCredentials(scheme, info);
	assert (scheme == "Basic");
	assert (info == "dXNlcjpzZWNyZXQ=");
}


void HTTPCredentialsTest::testBadCredentials()
{
	HTTPRequest request;
	
	std::string scheme;
	std::string info;
	try
	{
		request.getCredentials(scheme, info);
		fail("no credentials - must throw");
	}
	catch (NotAuthenticatedException&)
	{
	}
	
	request.setCredentials("Test", "SomeData");
	request.getCredentials(scheme, info);
	assert (scheme == "Test");
	assert (info == "SomeData");
	
	try
	{
		HTTPBasicCredentials cred(request);
		fail("bad scheme - must throw");
	}
	catch (NotAuthenticatedException&)
	{
	}
}


void HTTPCredentialsTest::testAuthenticationParams()
{
	const std::string authInfo("nonce=\"212573bb90170538efad012978ab811f%lu\", realm=\"TestDigest\", response=\"40e4889cfbd0e561f71e3107a2863bc4\", uri=\"/digest/\", username=\"user\"");
	HTTPAuthenticationParams params(authInfo);
	
	assert (params["nonce"] == "212573bb90170538efad012978ab811f%lu");
	assert (params["realm"] == "TestDigest");
	assert (params["response"] == "40e4889cfbd0e561f71e3107a2863bc4");
	assert (params["uri"] == "/digest/");
	assert (params["username"] == "user");
	assert (params.size() == 5);
	assert (params.toString() == authInfo);
	
	params.clear();
	HTTPRequest request;
	request.set("Authorization", "Digest " + authInfo);
	params.fromRequest(request);

	assert (params["nonce"] == "212573bb90170538efad012978ab811f%lu");
	assert (params["realm"] == "TestDigest");
	assert (params["response"] == "40e4889cfbd0e561f71e3107a2863bc4");
	assert (params["uri"] == "/digest/");
	assert (params["username"] == "user");
	assert (params.size() == 5);

	params.clear();
	HTTPResponse response;
	response.set("WWW-Authenticate", "Digest realm=\"TestDigest\", nonce=\"212573bb90170538efad012978ab811f%lu\"");	
	params.fromResponse(response);
	
	assert (params["realm"] == "TestDigest");
	assert (params["nonce"] == "212573bb90170538efad012978ab811f%lu");
	assert (params.size() == 2);
}


void HTTPCredentialsTest::testAuthenticationParamsMultipleHeaders()
{
	HTTPResponse response;
	response.add("WWW-Authenticate", "Unsupported realm=\"TestUnsupported\"");	
	response.add("WWW-Authenticate", "Digest realm=\"TestDigest\", nonce=\"212573bb90170538efad012978ab811f%lu\"");	
	HTTPAuthenticationParams params(response);
	
	assert (params["realm"] == "TestDigest");
	assert (params["nonce"] == "212573bb90170538efad012978ab811f%lu");
	assert (params.size() == 2);
}


void HTTPCredentialsTest::testDigestCredentials()
{
	HTTPDigestCredentials creds("user", "s3cr3t");
	HTTPRequest request(HTTPRequest::HTTP_GET, "/digest/");
	HTTPResponse response;
	response.set("WWW-Authenticate", "Digest realm=\"TestDigest\", nonce=\"212573bb90170538efad012978ab811f%lu\"");	
	creds.authenticate(request, response);
	std::string auth = request.get("Authorization");
	assert (auth == "Digest username=\"user\", nonce=\"212573bb90170538efad012978ab811f%lu\", realm=\"TestDigest\", uri=\"/digest/\", response=\"40e4889cfbd0e561f71e3107a2863bc4\"");
}


void HTTPCredentialsTest::testDigestCredentialsQoP()
{
	HTTPDigestCredentials creds("user", "s3cr3t");
	HTTPRequest request(HTTPRequest::HTTP_GET, "/digest/");
	HTTPResponse response;
	response.set("WWW-Authenticate", "Digest realm=\"TestDigest\", nonce=\"212573bb90170538efad012978ab811f%lu\", opaque=\"opaque\", qop=\"auth,auth-int\"");	
	creds.authenticate(request, response);
	
	HTTPAuthenticationParams params(request);
	assert (params["nonce"] == "212573bb90170538efad012978ab811f%lu");
	assert (params["realm"] == "TestDigest");
	assert (params["response"] != "40e4889cfbd0e561f71e3107a2863bc4");
	assert (params["uri"] == "/digest/");
	assert (params["username"] == "user");
	assert (params["opaque"] == "opaque");
	assert (params["cnonce"] != "");
	assert (params["nc"] == "00000001");
	assert (params["qop"] == "auth");
	assert (params.size() == 9);
	
	std::string cnonce = params["cnonce"];
	std::string aresp = params["response"];
	
	params.clear();
	
	creds.updateAuthInfo(request);
	params.fromRequest(request);
	assert (params["nonce"] == "212573bb90170538efad012978ab811f%lu");
	assert (params["realm"] == "TestDigest");
	assert (params["response"] != aresp);
	assert (params["uri"] == "/digest/");
	assert (params["username"] == "user");
	assert (params["opaque"] == "opaque");
	assert (params["cnonce"] == cnonce);
	assert (params["nc"] == "00000002");
	assert (params["qop"] == "auth");
	assert (params.size() == 9);
}


void HTTPCredentialsTest::testCredentialsBasic()
{
	HTTPCredentials creds("user", "s3cr3t");
	HTTPRequest request(HTTPRequest::HTTP_GET, "/basic/");
	HTTPResponse response;
	response.set("WWW-Authenticate", "Basic realm=\"TestBasic\"");	
	creds.authenticate(request, response);	
	assert (request.get("Authorization") == "Basic dXNlcjpzM2NyM3Q=");
}


void HTTPCredentialsTest::testProxyCredentialsBasic()
{
	HTTPCredentials creds("user", "s3cr3t");
	HTTPRequest request(HTTPRequest::HTTP_GET, "/basic/");
	HTTPResponse response;
	response.set("Proxy-Authenticate", "Basic realm=\"TestBasic\"");	
	creds.proxyAuthenticate(request, response);	
	assert (request.get("Proxy-Authorization") == "Basic dXNlcjpzM2NyM3Q=");
}


void HTTPCredentialsTest::testCredentialsDigest()
{
	HTTPCredentials creds("user", "s3cr3t");
	HTTPRequest request(HTTPRequest::HTTP_GET, "/digest/");
	HTTPResponse response;
	response.set("WWW-Authenticate", "Digest realm=\"TestDigest\", nonce=\"212573bb90170538efad012978ab811f%lu\"");	
	creds.authenticate(request, response);
	std::string auth = request.get("Authorization");
	assert (auth == "Digest username=\"user\", nonce=\"212573bb90170538efad012978ab811f%lu\", realm=\"TestDigest\", uri=\"/digest/\", response=\"40e4889cfbd0e561f71e3107a2863bc4\"");
}


void HTTPCredentialsTest::testCredentialsDigestMultipleHeaders()
{
	HTTPCredentials creds("user", "s3cr3t");
	HTTPRequest request(HTTPRequest::HTTP_GET, "/digest/");
	HTTPResponse response;
	response.add("WWW-Authenticate", "Unsupported realm=\"TestUnsupported\"");	
	response.add("WWW-Authenticate", "Digest realm=\"TestDigest\", nonce=\"212573bb90170538efad012978ab811f%lu\"");	
	creds.authenticate(request, response);
	std::string auth = request.get("Authorization");
	assert (auth == "Digest username=\"user\", nonce=\"212573bb90170538efad012978ab811f%lu\", realm=\"TestDigest\", uri=\"/digest/\", response=\"40e4889cfbd0e561f71e3107a2863bc4\"");
}


void HTTPCredentialsTest::testProxyCredentialsDigest()
{
	HTTPCredentials creds("user", "s3cr3t");
	HTTPRequest request(HTTPRequest::HTTP_GET, "/digest/");
	HTTPResponse response;
	response.set("Proxy-Authenticate", "Digest realm=\"TestDigest\", nonce=\"212573bb90170538efad012978ab811f%lu\"");	
	creds.proxyAuthenticate(request, response);	
	assert (request.get("Proxy-Authorization") == "Digest username=\"user\", nonce=\"212573bb90170538efad012978ab811f%lu\", realm=\"TestDigest\", uri=\"/digest/\", response=\"40e4889cfbd0e561f71e3107a2863bc4\"");
}


void HTTPCredentialsTest::testExtractCredentials()
{
	Poco::URI uri("http://user:s3cr3t@host.com/");
	std::string username;
	std::string password;
	HTTPCredentials::extractCredentials(uri, username, password);
	assert (username == "user");
	assert (password == "s3cr3t");
}


void HTTPCredentialsTest::testVerifyAuthInfo()
{
	HTTPDigestCredentials creds("user", "s3cr3t");
	HTTPRequest request(HTTPRequest::HTTP_GET, "/digest/");
	HTTPResponse response;
	response.set("WWW-Authenticate", "Digest realm=\"TestDigest\", nonce=\"212573bb90170538efad012978ab811f%lu\"");	
	creds.authenticate(request, response);
	assert (creds.verifyAuthInfo(request));
	
	request.set("Authorization", "Digest nonce=\"212573bb90170538efad012978ab811f%lu\", realm=\"TestDigest\", response=\"xxe4889cfbd0e561f71e3107a2863bc4\", uri=\"/digest/\", username=\"user\"");
	assert (!creds.verifyAuthInfo(request));
}


void HTTPCredentialsTest::testVerifyAuthInfoQoP()
{
	HTTPDigestCredentials creds("user", "s3cr3t");
	HTTPRequest request(HTTPRequest::HTTP_GET, "/digest/");
	HTTPResponse response;
	response.set("WWW-Authenticate", "Digest realm=\"TestDigest\", nonce=\"212573bb90170538efad012978ab811f%lu\", opaque=\"opaque\", qop=\"auth,auth-int\"");	
	creds.authenticate(request, response);
	assert (creds.verifyAuthInfo(request));
	
	request.set("Authorization", "Digest cnonce=\"f9c80ffd1c3bc4ee47ed92b704ba75a4\", nc=00000001, nonce=\"212573bb90170538efad012978ab811f%lu\", opaque=\"opaque\", qop=\"auth\", realm=\"TestDigest\", response=\"ff0e90b9aa019120ea0ed6e23ce95d9a\", uri=\"/digest/\", username=\"user\"");
	assert (!creds.verifyAuthInfo(request));
}


void HTTPCredentialsTest::setUp()
{
}


void HTTPCredentialsTest::tearDown()
{
}


CppUnit::Test* HTTPCredentialsTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("HTTPCredentialsTest");

	CppUnit_addTest(pSuite, HTTPCredentialsTest, testBasicCredentials);
	CppUnit_addTest(pSuite, HTTPCredentialsTest, testProxyBasicCredentials);
	CppUnit_addTest(pSuite, HTTPCredentialsTest, testBadCredentials);
	CppUnit_addTest(pSuite, HTTPCredentialsTest, testAuthenticationParams);
	CppUnit_addTest(pSuite, HTTPCredentialsTest, testAuthenticationParamsMultipleHeaders);
	CppUnit_addTest(pSuite, HTTPCredentialsTest, testDigestCredentials);
	CppUnit_addTest(pSuite, HTTPCredentialsTest, testDigestCredentialsQoP);
	CppUnit_addTest(pSuite, HTTPCredentialsTest, testCredentialsBasic);
	CppUnit_addTest(pSuite, HTTPCredentialsTest, testProxyCredentialsBasic);
	CppUnit_addTest(pSuite, HTTPCredentialsTest, testCredentialsDigest);
	CppUnit_addTest(pSuite, HTTPCredentialsTest, testCredentialsDigestMultipleHeaders);
	CppUnit_addTest(pSuite, HTTPCredentialsTest, testProxyCredentialsDigest);
	CppUnit_addTest(pSuite, HTTPCredentialsTest, testExtractCredentials);
	CppUnit_addTest(pSuite, HTTPCredentialsTest, testVerifyAuthInfo);
	CppUnit_addTest(pSuite, HTTPCredentialsTest, testVerifyAuthInfoQoP);

	return pSuite;
}
