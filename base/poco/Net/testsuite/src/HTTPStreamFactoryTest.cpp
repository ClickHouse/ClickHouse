//
// HTTPStreamFactoryTest.cpp
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
	URI uri("http://127.0.0.1/large");
	uri.setPort(server.port());
#ifndef POCO_ENABLE_CPP11
	std::auto_ptr<std::istream> pStr(factory.open(uri));
#else
	std::unique_ptr<std::istream> pStr(factory.open(uri));
#endif // POCO_ENABLE_CPP11
	std::ostringstream ostr;
	StreamCopier::copyStream(*pStr.get(), ostr);
	assert (ostr.str() == HTTPTestServer::LARGE_BODY);
}


void HTTPStreamFactoryTest::testEmptyPath()
{
	HTTPTestServer server;
	HTTPStreamFactory factory;
	URI uri("http://127.0.0.1");
	uri.setPort(server.port());
#ifndef POCO_ENABLE_CPP11
	std::auto_ptr<std::istream> pStr(factory.open(uri));
#else
	std::unique_ptr<std::istream> pStr(factory.open(uri));
#endif // POCO_ENABLE_CPP11
	std::ostringstream ostr;
	StreamCopier::copyStream(*pStr.get(), ostr);
	assert (ostr.str() == HTTPTestServer::SMALL_BODY);
}


void HTTPStreamFactoryTest::testRedirect()
{
	HTTPTestServer server;
	Poco::URIStreamOpener opener;
	opener.registerStreamFactory("http", new HTTPStreamFactory);
	URI uri("http://127.0.0.1/redirect");
	uri.setPort(server.port());
#ifndef POCO_ENABLE_CPP11
	std::auto_ptr<std::istream> pStr(opener.open(uri));
#else
	std::unique_ptr<std::istream> pStr(opener.open(uri));
#endif // POCO_ENABLE_CPP11
	std::ostringstream ostr;
	StreamCopier::copyStream(*pStr.get(), ostr);
	assert (ostr.str() == HTTPTestServer::LARGE_BODY);
}


void HTTPStreamFactoryTest::testProxy()
{
	HTTPTestServer server;
	HTTPStreamFactory factory("127.0.0.1", server.port());
	URI uri("http://www.somehost.com/large");
#ifndef POCO_ENABLE_CPP11
	std::auto_ptr<std::istream> pStr(factory.open(uri));
#else
	std::unique_ptr<std::istream> pStr(factory.open(uri));
#endif // POCO_ENABLE_CPP11
	std::ostringstream ostr;
	StreamCopier::copyStream(*pStr.get(), ostr);
	assert (ostr.str() == HTTPTestServer::LARGE_BODY);
}


void HTTPStreamFactoryTest::testError()
{
	HTTPTestServer server;
	HTTPStreamFactory factory;
	URI uri("http://127.0.0.1/notfound");
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
