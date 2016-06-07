//
// OAuth10CredentialsTest.cpp
//
// $Id$
//
// Copyright (c) 2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "OAuth10CredentialsTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Net/HTTPRequest.h"
#include "Poco/Net/HTTPResponse.h"
#include "Poco/Net/OAuth10Credentials.h"
#include "Poco/Net/NetException.h"
#include "Poco/Net/HTMLForm.h"
#include "Poco/URI.h"


using Poco::Net::HTTPRequest;
using Poco::Net::HTTPResponse;
using Poco::Net::OAuth10Credentials;
using Poco::Net::NotAuthenticatedException;
using Poco::Net::HTMLForm;
using Poco::URI;


OAuth10CredentialsTest::OAuth10CredentialsTest(const std::string& name): CppUnit::TestCase(name)
{
}


OAuth10CredentialsTest::~OAuth10CredentialsTest()
{
}


void OAuth10CredentialsTest::testCallback()
{
	// Note: Request taken from <https://dev.twitter.com/web/sign-in/implementing>
	//
	// POST /oauth/request_token HTTP/1.1
	// Host: api.twitter.com
	// Authorization: 
	//         OAuth oauth_callback="http%3A%2F%2Flocalhost%2Fsign-in-with-twitter%2F",
	//               oauth_consumer_key="cChZNFj6T5R0TigYB9yd1w",
	//               oauth_nonce="ea9ec8429b68d6b77cd5600adbbb0456",
	//               oauth_signature="F1Li3tvehgcraF8DMJ7OyxO4w9Y%3D",
	//               oauth_signature_method="HMAC-SHA1",
	//               oauth_timestamp="1318467427",
	//               oauth_version="1.0"

	
	URI uri("https://api.twitter.com/oauth/request_token");
	OAuth10Credentials creds("cChZNFj6T5R0TigYB9yd1w", "L8qq9PZyRg6ieKGEKhZolGC0vJWLw8iEJ88DRdyOg");
	creds.setCallback("http://localhost/sign-in-with-twitter/");
	creds.nonceAndTimestampForTesting("ea9ec8429b68d6b77cd5600adbbb0456", "1318467427");
	HTTPRequest request(HTTPRequest::HTTP_POST, uri.getPathEtc());
		
	creds.authenticate(request, uri);

	std::string auth = request.get("Authorization");	
	assert (auth == "OAuth"
		" oauth_consumer_key=\"cChZNFj6T5R0TigYB9yd1w\","
		" oauth_nonce=\"ea9ec8429b68d6b77cd5600adbbb0456\","
		" oauth_signature=\"F1Li3tvehgcraF8DMJ7OyxO4w9Y%3D\","
		" oauth_signature_method=\"HMAC-SHA1\","
		" oauth_timestamp=\"1318467427\","
		" oauth_callback=\"http%3A%2F%2Flocalhost%2Fsign-in-with-twitter%2F\","
		" oauth_version=\"1.0\"");
}


void OAuth10CredentialsTest::testParams()
{
	// Note: Request taken from <https://dev.twitter.com/oauth/overview/authorizing-requests>
	// and <https://dev.twitter.com/oauth/overview/creating-signatures>.
	// 
	// POST /1/statuses/update.json?include_entities=true HTTP/1.1
	// Content-Type: application/x-www-form-urlencoded
	// Authorization: 
	//         OAuth oauth_consumer_key="xvz1evFS4wEEPTGEFPHBog", 
	//               oauth_nonce="kYjzVBB8Y0ZFabxSWbWovY3uYSQ2pTgmZeNu2VS4cg", 
	//               oauth_signature="tnnArxj06cWHq44gCs1OSKk%2FjLY%3D", 
	//               oauth_signature_method="HMAC-SHA1", 
	//               oauth_timestamp="1318622958", 
	//               oauth_token="370773112-GmHxMAgYyLbNEtIKZeRNFsMKPR9EyMZeS9weJAEb", 
	//               oauth_version="1.0"
	// Content-Length: 76
	// Host: api.twitter.com
	// 
	// status=Hello%20Ladies%20%2b%20Gentlemen%2c%20a%20signed%20OAuth%20request%21

	URI uri("https://api.twitter.com/1/statuses/update.json?include_entities=true");
	OAuth10Credentials creds(
		"xvz1evFS4wEEPTGEFPHBog", 
		"kAcSOqF21Fu85e7zjz7ZN2U4ZRhfV3WpwPAoE3Z7kBw", 
		"370773112-GmHxMAgYyLbNEtIKZeRNFsMKPR9EyMZeS9weJAEb", 
		"LswwdoUaIvS8ltyTt5jkRh4J50vUPVVHtR2YPi5kE"
	);
	creds.nonceAndTimestampForTesting("kYjzVBB8Y0ZFabxSWbWovY3uYSQ2pTgmZeNu2VS4cg", "1318622958");
	HTTPRequest request(HTTPRequest::HTTP_POST, uri.getPathEtc());
	
	HTMLForm params;
	params.set("include_entities", "true");
	params.set("status", "Hello Ladies + Gentlemen, a signed OAuth request!");
	
	creds.authenticate(request, uri, params);
	
	std::string auth = request.get("Authorization");
	assert (auth == "OAuth"
		" oauth_consumer_key=\"xvz1evFS4wEEPTGEFPHBog\","
		" oauth_nonce=\"kYjzVBB8Y0ZFabxSWbWovY3uYSQ2pTgmZeNu2VS4cg\","
		" oauth_signature=\"tnnArxj06cWHq44gCs1OSKk%2FjLY%3D\","
		" oauth_signature_method=\"HMAC-SHA1\","
		" oauth_timestamp=\"1318622958\","
		" oauth_token=\"370773112-GmHxMAgYyLbNEtIKZeRNFsMKPR9EyMZeS9weJAEb\","
		" oauth_version=\"1.0\"");
}


void OAuth10CredentialsTest::testRealm()
{
	// Note: Request taken from <https://dev.twitter.com/oauth/overview/authorizing-requests>
	// and <https://dev.twitter.com/oauth/overview/creating-signatures>.
	//
	// POST /1/statuses/update.json?include_entities=true HTTP/1.1
	// Content-Type: application/x-www-form-urlencoded
	// Authorization: 
	//         OAuth realm="Twitter API"
	//               oauth_consumer_key="xvz1evFS4wEEPTGEFPHBog", 
	//               oauth_nonce="kYjzVBB8Y0ZFabxSWbWovY3uYSQ2pTgmZeNu2VS4cg", 
	//               oauth_signature="tnnArxj06cWHq44gCs1OSKk%2FjLY%3D", 
	//               oauth_signature_method="HMAC-SHA1", 
	//               oauth_timestamp="1318622958", 
	//               oauth_token="370773112-GmHxMAgYyLbNEtIKZeRNFsMKPR9EyMZeS9weJAEb", 
	//               oauth_version="1.0"
	// Content-Length: 76
	// Host: api.twitter.com
	// 
	// status=Hello%20Ladies%20%2b%20Gentlemen%2c%20a%20signed%20OAuth%20request%21

	URI uri("https://api.twitter.com/1/statuses/update.json?include_entities=true");
	OAuth10Credentials creds(
		"xvz1evFS4wEEPTGEFPHBog", 
		"kAcSOqF21Fu85e7zjz7ZN2U4ZRhfV3WpwPAoE3Z7kBw", 
		"370773112-GmHxMAgYyLbNEtIKZeRNFsMKPR9EyMZeS9weJAEb", 
		"LswwdoUaIvS8ltyTt5jkRh4J50vUPVVHtR2YPi5kE"
	);
	creds.setRealm("Twitter API");
	creds.nonceAndTimestampForTesting("kYjzVBB8Y0ZFabxSWbWovY3uYSQ2pTgmZeNu2VS4cg", "1318622958");
	HTTPRequest request(HTTPRequest::HTTP_POST, uri.getPathEtc());
	
	HTMLForm params;
	params.set("include_entities", "true");
	params.set("status", "Hello Ladies + Gentlemen, a signed OAuth request!");
	
	creds.authenticate(request, uri, params);
	
	std::string auth = request.get("Authorization");
	assert (auth == "OAuth"
		" realm=\"Twitter API\","
		" oauth_consumer_key=\"xvz1evFS4wEEPTGEFPHBog\","
		" oauth_nonce=\"kYjzVBB8Y0ZFabxSWbWovY3uYSQ2pTgmZeNu2VS4cg\","
		" oauth_signature=\"tnnArxj06cWHq44gCs1OSKk%2FjLY%3D\","
		" oauth_signature_method=\"HMAC-SHA1\","
		" oauth_timestamp=\"1318622958\","
		" oauth_token=\"370773112-GmHxMAgYyLbNEtIKZeRNFsMKPR9EyMZeS9weJAEb\","
		" oauth_version=\"1.0\"");
}


void OAuth10CredentialsTest::testPlaintext()
{
	URI uri("https://api.twitter.com/oauth/request_token");
	OAuth10Credentials creds("consumerKey", "consumerSecret");
	creds.setCallback("http://localhost/sign-in-with-twitter/");
	HTTPRequest request(HTTPRequest::HTTP_POST, uri.getPathEtc());
		
	creds.authenticate(request, uri, OAuth10Credentials::SIGN_PLAINTEXT);

	std::string auth = request.get("Authorization");	
	
	assert (auth == "OAuth"
		" oauth_consumer_key=\"consumerKey\","
		" oauth_signature=\"consumerSecret%26\","
		" oauth_signature_method=\"PLAINTEXT\","
		" oauth_callback=\"http%3A%2F%2Flocalhost%2Fsign-in-with-twitter%2F\","
		" oauth_version=\"1.0\"");
}


void OAuth10CredentialsTest::testVerify()
{
	URI uri("https://api.twitter.com/1/statuses/update.json?include_entities=true");
	HTTPRequest request(HTTPRequest::HTTP_POST, uri.getPathEtc());
	request.set("Authorization", "OAuth"
		" oauth_consumer_key=\"xvz1evFS4wEEPTGEFPHBog\","
		" oauth_nonce=\"kYjzVBB8Y0ZFabxSWbWovY3uYSQ2pTgmZeNu2VS4cg\","
		" oauth_signature=\"tnnArxj06cWHq44gCs1OSKk%2FjLY%3D\","
		" oauth_signature_method=\"HMAC-SHA1\","
		" oauth_timestamp=\"1318622958\","
		" oauth_token=\"370773112-GmHxMAgYyLbNEtIKZeRNFsMKPR9EyMZeS9weJAEb\","
		" oauth_version=\"1.0\"");
	
	OAuth10Credentials creds(request);
	assert (creds.getConsumerKey() == "xvz1evFS4wEEPTGEFPHBog");
	assert (creds.getToken() == "370773112-GmHxMAgYyLbNEtIKZeRNFsMKPR9EyMZeS9weJAEb");
	creds.setConsumerSecret("kAcSOqF21Fu85e7zjz7ZN2U4ZRhfV3WpwPAoE3Z7kBw");
	creds.setTokenSecret("LswwdoUaIvS8ltyTt5jkRh4J50vUPVVHtR2YPi5kE");
	
	HTMLForm params;
	params.read(uri.getRawQuery());
	params.read("status=Hello%20Ladies%20%2b%20Gentlemen%2c%20a%20signed%20OAuth%20request%21");
	
	assert (creds.verify(request, uri, params));
}


void OAuth10CredentialsTest::testVerifyPlaintext()
{
	URI uri("https://api.twitter.com/oauth/request_token");
	HTTPRequest request(HTTPRequest::HTTP_POST, uri.getPathEtc());
	request.set("Authorization", "OAuth"
		" oauth_consumer_key=\"consumerKey\","
		" oauth_signature=\"consumerSecret%26\","
		" oauth_signature_method=\"PLAINTEXT\","
		" oauth_callback=\"http%3A%2F%2Flocalhost%2Fsign-in-with-twitter%2F\","
		" oauth_version=\"1.0\"");
	
	OAuth10Credentials creds(request);
	assert (creds.getConsumerKey() == "consumerKey");
	creds.setConsumerSecret("consumerSecret");
	
	assert (creds.verify(request, uri));
	assert (creds.getCallback() == "http://localhost/sign-in-with-twitter/");
}


void OAuth10CredentialsTest::setUp()
{
}


void OAuth10CredentialsTest::tearDown()
{
}


CppUnit::Test* OAuth10CredentialsTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("OAuth10CredentialsTest");

	CppUnit_addTest(pSuite, OAuth10CredentialsTest, testCallback);
	CppUnit_addTest(pSuite, OAuth10CredentialsTest, testParams);
	CppUnit_addTest(pSuite, OAuth10CredentialsTest, testRealm);
	CppUnit_addTest(pSuite, OAuth10CredentialsTest, testPlaintext);
	CppUnit_addTest(pSuite, OAuth10CredentialsTest, testVerify);
	CppUnit_addTest(pSuite, OAuth10CredentialsTest, testVerifyPlaintext);

	return pSuite;
}
