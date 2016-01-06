//
// HTTPStreamFactoryTest.cpp
//
// $Id: //poco/1.4/Net/testsuite/src/HTTPStreamFactoryTest.cpp#1 $
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "HTTPStreamFactoryTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Net/HTTPStreamFactory.h"
#include "Poco/Net/NetException.h"
#include "Poco/URI.h"
#include "Poco/URIStreamOpener.h"
#include "Poco/StreamCopier.h"
#include "HTTPTestServer.h"
#include <sstream>
#include <memory>


using Poco::Net::HTTPStreamFactory;
using Poco::Net::NetException;
using Poco::Net::HTTPException;
using Poco::URI;
using Poco::StreamCopier;


HTTPStreamFactoryTest::HTTPStreamFactoryTest(const std::string& name): CppUnit::TestCase(name)
{
}


HTTPStreamFactoryTest::~HTTPStreamFactoryTest()
{
}


void HTTPStreamFactoryTest::testNoRedirect()
{
	HTTPTestServer server;
	HTTPStreamFactory factory;
	URI uri("http://localhost/large");
	uri.setPort(server.port());
	std::auto_ptr<std::istream> pStr(factory.open(uri));
	std::ostringstream ostr;
	StreamCopier::copyStream(*pStr.get(), ostr);
	assert (ostr.str() == HTTPTestServer::LARGE_BODY);
}


void HTTPStreamFactoryTest::testEmptyPath()
{
	HTTPTestServer server;
	HTTPStreamFactory factory;
	URI uri("http://localhost");
	uri.setPort(server.port());
	std::auto_ptr<std::istream> pStr(factory.open(uri));
	std::ostringstream ostr;
	StreamCopier::copyStream(*pStr.get(), ostr);
	assert (ostr.str() == HTTPTestServer::SMALL_BODY);
}


void HTTPStreamFactoryTest::testRedirect()
{
	HTTPTestServer server;
	Poco::URIStreamOpener opener;
	opener.registerStreamFactory("http", new HTTPStreamFactory);
	URI uri("http://localhost/redirect");
	uri.setPort(server.port());
	std::auto_ptr<std::istream> pStr(opener.open(uri));
	std::ostringstream ostr;
	StreamCopier::copyStream(*pStr.get(), ostr);
	assert (ostr.str() == HTTPTestServer::LARGE_BODY);
}


void HTTPStreamFactoryTest::testProxy()
{
	HTTPTestServer server;
	HTTPStreamFactory factory("localhost", server.port());
	URI uri("http://www.somehost.com/large");
	std::auto_ptr<std::istream> pStr(factory.open(uri));
	std::ostringstream ostr;
	StreamCopier::copyStream(*pStr.get(), ostr);
	assert (ostr.str() == HTTPTestServer::LARGE_BODY);
}


void HTTPStreamFactoryTest::testError()
{
	HTTPTestServer server;
	HTTPStreamFactory factory;
	URI uri("http://localhost/notfound");
	uri.setPort(server.port());
	try
	{
		std::istream* pStr = factory.open(uri);
		fail("not found - must throw");
		pStr = pStr + 0; // to silence gcc
	}
	catch (HTTPException& exc)
	{
		std::string m = exc.displayText();
	}
}


void HTTPStreamFactoryTest::setUp()
{
}


void HTTPStreamFactoryTest::tearDown()
{
}


CppUnit::Test* HTTPStreamFactoryTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("HTTPStreamFactoryTest");

	CppUnit_addTest(pSuite, HTTPStreamFactoryTest, testNoRedirect);
	CppUnit_addTest(pSuite, HTTPStreamFactoryTest, testEmptyPath);
	CppUnit_addTest(pSuite, HTTPStreamFactoryTest, testRedirect);
	CppUnit_addTest(pSuite, HTTPStreamFactoryTest, testProxy);
	CppUnit_addTest(pSuite, HTTPStreamFactoryTest, testError);

	return pSuite;
}
