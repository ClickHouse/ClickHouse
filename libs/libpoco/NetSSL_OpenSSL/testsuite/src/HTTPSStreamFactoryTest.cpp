//
// HTTPSStreamFactoryTest.cpp
//
// $Id: //poco/1.4/NetSSL_OpenSSL/testsuite/src/HTTPSStreamFactoryTest.cpp#1 $
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "HTTPSStreamFactoryTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Net/HTTPSStreamFactory.h"
#include "Poco/Net/NetException.h"
#include "Poco/Util/Application.h"
#include "Poco/Util/AbstractConfiguration.h"
#include "Poco/URI.h"
#include "Poco/Exception.h"
#include "Poco/StreamCopier.h"
#include "HTTPSTestServer.h"
#include <sstream>
#include <memory>


using Poco::Net::HTTPSStreamFactory;
using Poco::Net::NetException;
using Poco::Net::HTTPException;
using Poco::Util::Application;
using Poco::URI;
using Poco::StreamCopier;


HTTPSStreamFactoryTest::HTTPSStreamFactoryTest(const std::string& name): CppUnit::TestCase(name)
{
}


HTTPSStreamFactoryTest::~HTTPSStreamFactoryTest()
{
}


void HTTPSStreamFactoryTest::testNoRedirect()
{
	HTTPSTestServer server;
	HTTPSStreamFactory factory;
	URI uri("https://localhost/large");
	uri.setPort(server.port());
	std::auto_ptr<std::istream> pStr(factory.open(uri));
	std::ostringstream ostr;
	StreamCopier::copyStream(*pStr.get(), ostr);
	assert (ostr.str() == HTTPSTestServer::LARGE_BODY);
}


void HTTPSStreamFactoryTest::testEmptyPath()
{
	HTTPSTestServer server;
	HTTPSStreamFactory factory;
	URI uri("https://localhost");
	uri.setPort(server.port());
	std::auto_ptr<std::istream> pStr(factory.open(uri));
	std::ostringstream ostr;
	StreamCopier::copyStream(*pStr.get(), ostr);
	assert (ostr.str() == HTTPSTestServer::SMALL_BODY);
}


void HTTPSStreamFactoryTest::testRedirect()
{
	HTTPSTestServer server;
	HTTPSStreamFactory factory;
	URI uri("https://localhost/redirect");
	uri.setPort(server.port());
	std::auto_ptr<std::istream> pStr(factory.open(uri));
	std::ostringstream ostr;
	StreamCopier::copyStream(*pStr.get(), ostr);
	assert (ostr.str() == HTTPSTestServer::LARGE_BODY);
}


void HTTPSStreamFactoryTest::testProxy()
{
	HTTPSTestServer server;
	HTTPSStreamFactory factory(
		Application::instance().config().getString("testsuite.proxy.host"), 
		Application::instance().config().getInt("testsuite.proxy.port")
	);
	URI uri("https://secure.appinf.com/public/poco/NetSSL.txt");
	std::auto_ptr<std::istream> pStr(factory.open(uri));
	std::ostringstream ostr;
	StreamCopier::copyStream(*pStr.get(), ostr);
	assert (ostr.str().length() > 0);
}


void HTTPSStreamFactoryTest::testError()
{
	HTTPSTestServer server;
	HTTPSStreamFactory factory;
	URI uri("https://localhost/notfound");
	uri.setPort(server.port());
	try
	{
		std::istream* pStr = factory.open(uri);
		fail("not found - must throw");
	}
	catch (HTTPException& exc)
	{
		std::string m = exc.displayText();
	}
}


void HTTPSStreamFactoryTest::setUp()
{
}


void HTTPSStreamFactoryTest::tearDown()
{
}


CppUnit::Test* HTTPSStreamFactoryTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("HTTPSStreamFactoryTest");

	CppUnit_addTest(pSuite, HTTPSStreamFactoryTest, testNoRedirect);
	CppUnit_addTest(pSuite, HTTPSStreamFactoryTest, testEmptyPath);
	CppUnit_addTest(pSuite, HTTPSStreamFactoryTest, testRedirect);
	CppUnit_addTest(pSuite, HTTPSStreamFactoryTest, testProxy);
	CppUnit_addTest(pSuite, HTTPSStreamFactoryTest, testError);

	return pSuite;
}
