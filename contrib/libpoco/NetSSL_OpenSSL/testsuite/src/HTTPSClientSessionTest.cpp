//
// HTTPSClientSessionTest.cpp
//
// $Id: //poco/1.4/NetSSL_OpenSSL/testsuite/src/HTTPSClientSessionTest.cpp#1 $
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "HTTPSClientSessionTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Net/HTTPSClientSession.h"
#include "Poco/Net/HTTPRequest.h"
#include "Poco/Net/HTTPRequestHandler.h"
#include "Poco/Net/HTTPRequestHandlerFactory.h"
#include "Poco/Net/HTTPResponse.h"
#include "Poco/Net/HTTPServer.h"
#include "Poco/Net/HTTPServerResponse.h"
#include "Poco/Net/HTTPServerRequest.h"
#include "Poco/Net/HTTPServerParams.h"
#include "Poco/Net/SecureStreamSocket.h"
#include "Poco/Net/Context.h"
#include "Poco/Net/Session.h"
#include "Poco/Net/SSLManager.h"
#include "Poco/Net/SSLException.h"
#include "Poco/Util/Application.h"
#include "Poco/Util/AbstractConfiguration.h"
#include "Poco/StreamCopier.h"
#include "Poco/Exception.h"
#include "Poco/DateTimeFormatter.h"
#include "Poco/DateTimeFormat.h"
#include "Poco/Thread.h"
#include "HTTPSTestServer.h"
#include <istream>
#include <ostream>
#include <sstream>


using namespace Poco::Net;
using Poco::Util::Application;
using Poco::StreamCopier;
using Poco::Thread;


class TestRequestHandler: public HTTPRequestHandler
	/// Return a HTML document with the current date and time.
{
public:
	TestRequestHandler()
	{
	}
	
	void handleRequest(HTTPServerRequest& request, HTTPServerResponse& response)
	{
		response.setChunkedTransferEncoding(true);
		response.setContentType(request.getContentType());
		std::ostream& ostr = response.send();
		Poco::StreamCopier::copyStream(request.stream(), ostr);
	}

};


class TestRequestHandlerFactory: public HTTPRequestHandlerFactory
{
public:
	TestRequestHandlerFactory()
	{
	}

	HTTPRequestHandler* createRequestHandler(const HTTPServerRequest& request)
	{
		return new TestRequestHandler();
	}
};


HTTPSClientSessionTest::HTTPSClientSessionTest(const std::string& name): CppUnit::TestCase(name)
{
}


HTTPSClientSessionTest::~HTTPSClientSessionTest()
{
}


void HTTPSClientSessionTest::testGetSmall()
{
	HTTPSTestServer srv;
	HTTPSClientSession s("localhost", srv.port());
	HTTPRequest request(HTTPRequest::HTTP_GET, "/small");
	s.sendRequest(request);
	HTTPResponse response;
	std::istream& rs = s.receiveResponse(response);
	assert (response.getContentLength() == HTTPSTestServer::SMALL_BODY.length());
	assert (response.getContentType() == "text/plain");
	std::ostringstream ostr;
	StreamCopier::copyStream(rs, ostr);
	assert (ostr.str() == HTTPSTestServer::SMALL_BODY);
}


void HTTPSClientSessionTest::testGetLarge()
{
	HTTPSTestServer srv;
	HTTPSClientSession s("localhost", srv.port());
	HTTPRequest request(HTTPRequest::HTTP_GET, "/large");
	s.sendRequest(request);
	HTTPResponse response;
	std::istream& rs = s.receiveResponse(response);
	assert (response.getContentLength() == HTTPSTestServer::LARGE_BODY.length());
	assert (response.getContentType() == "text/plain");
	std::ostringstream ostr;
	StreamCopier::copyStream(rs, ostr);
	assert (ostr.str() == HTTPSTestServer::LARGE_BODY);
}


void HTTPSClientSessionTest::testHead()
{
	HTTPSTestServer srv;
	HTTPSClientSession s("localhost", srv.port());
	HTTPRequest request(HTTPRequest::HTTP_HEAD, "/large");
	s.sendRequest(request);
	HTTPResponse response;
	std::istream& rs = s.receiveResponse(response);
	assert (response.getContentLength() == HTTPSTestServer::LARGE_BODY.length());
	assert (response.getContentType() == "text/plain");
	std::ostringstream ostr;
	assert (StreamCopier::copyStream(rs, ostr) == 0);
}


void HTTPSClientSessionTest::testPostSmallIdentity()
{
	HTTPSTestServer srv;
	HTTPSClientSession s("localhost", srv.port());
	HTTPRequest request(HTTPRequest::HTTP_POST, "/echo");
	std::string body("this is a random request body\r\n0\r\n");
	request.setContentLength((int) body.length());
	s.sendRequest(request) << body;
	HTTPResponse response;
	std::istream& rs = s.receiveResponse(response);
	assert (response.getContentLength() == body.length());
	std::ostringstream ostr;
	StreamCopier::copyStream(rs, ostr);
	assert (ostr.str() == body);
}


void HTTPSClientSessionTest::testPostLargeIdentity()
{
	HTTPSTestServer srv;
	HTTPSClientSession s("localhost", srv.port());
	HTTPRequest request(HTTPRequest::HTTP_POST, "/echo");
	std::string body(8000, 'x');
	body.append("\r\n0\r\n");
	request.setContentLength((int) body.length());
	s.sendRequest(request) << body;
	HTTPResponse response;
	std::istream& rs = s.receiveResponse(response);
	assert (response.getContentLength() == body.length());
	std::ostringstream ostr;
	StreamCopier::copyStream(rs, ostr);
	assert (ostr.str() == body);
}


void HTTPSClientSessionTest::testPostSmallChunked()
{
	HTTPSTestServer srv;
	HTTPSClientSession s("localhost", srv.port());
	HTTPRequest request(HTTPRequest::HTTP_POST, "/echo");
	std::string body("this is a random request body");
	request.setChunkedTransferEncoding(true);
	s.sendRequest(request) << body;
	HTTPResponse response;
	std::istream& rs = s.receiveResponse(response);
	assert (response.getChunkedTransferEncoding());
	assert (response.getContentLength() == HTTPMessage::UNKNOWN_CONTENT_LENGTH);
	std::ostringstream ostr;
	StreamCopier::copyStream(rs, ostr);
	assert (ostr.str() == body);
}


void HTTPSClientSessionTest::testPostLargeChunked()
{
	HTTPSTestServer srv;
	HTTPSClientSession s("localhost", srv.port());
	HTTPRequest request(HTTPRequest::HTTP_POST, "/echo");
	std::string body(16000, 'x');
	request.setChunkedTransferEncoding(true);
	s.sendRequest(request) << body;
	HTTPResponse response;
	std::istream& rs = s.receiveResponse(response);
	assert (response.getChunkedTransferEncoding());
	assert (response.getContentLength() == HTTPMessage::UNKNOWN_CONTENT_LENGTH);
	std::ostringstream ostr;
	StreamCopier::copyStream(rs, ostr);
	assert (ostr.str() == body);
}


void HTTPSClientSessionTest::testPostLargeChunkedKeepAlive()
{
	SecureServerSocket svs(32322);
	HTTPServer srv(new TestRequestHandlerFactory(), svs, new HTTPServerParams());
	srv.start();
	try
	{
		HTTPSClientSession s("localhost", srv.port());
		s.setKeepAlive(true);
		for (int i = 0; i < 10; ++i)
		{
			HTTPRequest request(HTTPRequest::HTTP_POST, "/keepAlive", HTTPMessage::HTTP_1_1);
			std::string body(16000, 'x');
			request.setChunkedTransferEncoding(true);
			s.sendRequest(request) << body;
			HTTPResponse response;
			std::istream& rs = s.receiveResponse(response);
			assert (response.getChunkedTransferEncoding());
			assert (response.getContentLength() == HTTPMessage::UNKNOWN_CONTENT_LENGTH);
			std::ostringstream ostr;
			StreamCopier::copyStream(rs, ostr);
			assert (ostr.str() == body);
		}
		srv.stop();
	}
	catch (...)
	{
		srv.stop();
		throw;
	}
}


void HTTPSClientSessionTest::testKeepAlive()
{
	HTTPSTestServer srv;
	HTTPSClientSession s("localhost", srv.port());
	s.setKeepAlive(true);
	HTTPRequest request(HTTPRequest::HTTP_HEAD, "/keepAlive", HTTPMessage::HTTP_1_1);
	s.sendRequest(request);
	HTTPResponse response;
	std::istream& rs1 = s.receiveResponse(response);
	assert (response.getContentLength() == HTTPSTestServer::SMALL_BODY.length());
	assert (response.getContentType() == "text/plain");
	assert (response.getKeepAlive());
	std::ostringstream ostr1;
	assert (StreamCopier::copyStream(rs1, ostr1) == 0);
	
	request.setMethod(HTTPRequest::HTTP_GET);
	request.setURI("/small");
	s.sendRequest(request);
	std::istream& rs2 = s.receiveResponse(response);
	assert (response.getContentLength() == HTTPSTestServer::SMALL_BODY.length());
	assert (response.getKeepAlive());
	std::ostringstream ostr2;
	StreamCopier::copyStream(rs2, ostr2);
	assert (ostr2.str() == HTTPSTestServer::SMALL_BODY);
	
	request.setMethod(HTTPRequest::HTTP_GET);
	request.setURI("/large");
	s.sendRequest(request);
	std::istream& rs3 = s.receiveResponse(response);
	assert (response.getContentLength() == HTTPMessage::UNKNOWN_CONTENT_LENGTH);
	assert (response.getChunkedTransferEncoding());
	assert (response.getKeepAlive());
	std::ostringstream ostr3;
	StreamCopier::copyStream(rs3, ostr3);
	assert (ostr3.str() == HTTPSTestServer::LARGE_BODY);

	request.setMethod(HTTPRequest::HTTP_HEAD);
	request.setURI("/large");
	s.sendRequest(request);
	std::istream& rs4 = s.receiveResponse(response);
	assert (response.getContentLength() == HTTPSTestServer::LARGE_BODY.length());
	assert (response.getContentType() == "text/plain");
	assert (!response.getKeepAlive());
	std::ostringstream ostr4;
	assert (StreamCopier::copyStream(rs4, ostr4) == 0);
}


void HTTPSClientSessionTest::testInterop()
{
	HTTPSClientSession s("secure.appinf.com");
	HTTPRequest request(HTTPRequest::HTTP_GET, "/public/poco/NetSSL.txt");
	s.sendRequest(request);
	X509Certificate cert = s.serverCertificate();
	HTTPResponse response;
	std::istream& rs = s.receiveResponse(response);
	std::ostringstream ostr;
	StreamCopier::copyStream(rs, ostr);
	std::string str(ostr.str());
	assert (str == "This is a test file for NetSSL.\n");
	assert (cert.commonName() == "secure.appinf.com" || cert.commonName() == "*.appinf.com");
}


void HTTPSClientSessionTest::testProxy()
{
	HTTPSTestServer srv;
	HTTPSClientSession s("secure.appinf.com");
	s.setProxy(
		Application::instance().config().getString("testsuite.proxy.host"), 
		Application::instance().config().getInt("testsuite.proxy.port")
	);
	HTTPRequest request(HTTPRequest::HTTP_GET, "/public/poco/NetSSL.txt");
	s.sendRequest(request);
	X509Certificate cert = s.serverCertificate();
	HTTPResponse response;
	std::istream& rs = s.receiveResponse(response);
	std::ostringstream ostr;
	StreamCopier::copyStream(rs, ostr);
	std::string str(ostr.str());
	assert (str == "This is a test file for NetSSL.\n");
	assert (cert.commonName() == "secure.appinf.com" || cert.commonName() == "*.appinf.com");
}


void HTTPSClientSessionTest::testCachedSession()
{
	// ensure OpenSSL machinery is fully setup
	Context::Ptr pDefaultServerContext = SSLManager::instance().defaultServerContext();
	Context::Ptr pDefaultClientContext = SSLManager::instance().defaultClientContext();
	
	Context::Ptr pServerContext = new Context(
		Context::SERVER_USE, 
		Application::instance().config().getString("openSSL.server.privateKeyFile"),
		Application::instance().config().getString("openSSL.server.privateKeyFile"),
		Application::instance().config().getString("openSSL.server.caConfig"),
		Context::VERIFY_NONE,
		9,
		true,
		"ALL:!ADH:!LOW:!EXP:!MD5:@STRENGTH");	
	pServerContext->enableSessionCache(true, "TestSuite");
	pServerContext->setSessionTimeout(10);
	pServerContext->setSessionCacheSize(1000);
	pServerContext->disableStatelessSessionResumption();

	HTTPSTestServer srv(pServerContext);

	Context::Ptr pClientContext = new Context(
		Context::CLIENT_USE, 
		Application::instance().config().getString("openSSL.client.privateKeyFile"),
		Application::instance().config().getString("openSSL.client.privateKeyFile"),
		Application::instance().config().getString("openSSL.client.caConfig"),
		Context::VERIFY_RELAXED,
		9,
		true,
		"ALL:!ADH:!LOW:!EXP:!MD5:@STRENGTH");
	pClientContext->enableSessionCache(true);

	HTTPSClientSession s1("localhost", srv.port(), pClientContext);
	HTTPRequest request1(HTTPRequest::HTTP_GET, "/small");
	s1.sendRequest(request1);
	Session::Ptr pSession1 = s1.sslSession();
	HTTPResponse response1;
	std::istream& rs1 = s1.receiveResponse(response1);
	assert (response1.getContentLength() == HTTPSTestServer::SMALL_BODY.length());
	assert (response1.getContentType() == "text/plain");
	std::ostringstream ostr1;
	StreamCopier::copyStream(rs1, ostr1);
	assert (ostr1.str() == HTTPSTestServer::SMALL_BODY);

	HTTPSClientSession s2("localhost", srv.port(), pClientContext, pSession1);
	HTTPRequest request2(HTTPRequest::HTTP_GET, "/small");
	s2.sendRequest(request2);
	Session::Ptr pSession2 = s2.sslSession();
	HTTPResponse response2;
	std::istream& rs2 = s2.receiveResponse(response2);
	assert (response2.getContentLength() == HTTPSTestServer::SMALL_BODY.length());
	assert (response2.getContentType() == "text/plain");
	std::ostringstream ostr2;
	StreamCopier::copyStream(rs2, ostr2);
	assert (ostr2.str() == HTTPSTestServer::SMALL_BODY);
	
	assert (pSession1 == pSession2);

	HTTPRequest request3(HTTPRequest::HTTP_GET, "/small");
	s2.sendRequest(request3);
	Session::Ptr pSession3 = s2.sslSession();
	HTTPResponse response3;
	std::istream& rs3 = s2.receiveResponse(response3);
	assert (response3.getContentLength() == HTTPSTestServer::SMALL_BODY.length());
	assert (response3.getContentType() == "text/plain");
	std::ostringstream ostr3;
	StreamCopier::copyStream(rs3, ostr3);
	assert (ostr3.str() == HTTPSTestServer::SMALL_BODY);

	assert (pSession1 == pSession3);

	Thread::sleep(15000); // wait for session to expire
	pServerContext->flushSessionCache();
	
	HTTPRequest request4(HTTPRequest::HTTP_GET, "/small");
	s2.sendRequest(request4);
	Session::Ptr pSession4 = s2.sslSession();
	HTTPResponse response4;
	std::istream& rs4 = s2.receiveResponse(response4);
	assert (response4.getContentLength() == HTTPSTestServer::SMALL_BODY.length());
	assert (response4.getContentType() == "text/plain");
	std::ostringstream ostr4;
	StreamCopier::copyStream(rs4, ostr4);
	assert (ostr4.str() == HTTPSTestServer::SMALL_BODY);

	assert (pSession1 != pSession4);
}


void HTTPSClientSessionTest::testUnknownContentLength()
{
	HTTPSTestServer srv;
	HTTPSClientSession s("localhost", srv.port());
	HTTPRequest request(HTTPRequest::HTTP_GET, "/nolength");
	s.sendRequest(request);
	HTTPResponse response;
	std::istream& rs = s.receiveResponse(response);
	assert (response.getContentLength() == HTTPMessage::UNKNOWN_CONTENT_LENGTH);
	assert (response.getContentType() == "text/plain");
	std::ostringstream ostr;
	StreamCopier::copyStream(rs, ostr);
	assert (ostr.str() == HTTPSTestServer::SMALL_BODY);
}


void HTTPSClientSessionTest::testServerAbort()
{
	HTTPSTestServer srv;
	HTTPSClientSession s("localhost", srv.port());
	HTTPRequest request(HTTPRequest::HTTP_GET, "/nolength/connection/abort");
	s.sendRequest(request);
	HTTPResponse response;
	std::istream& rs = s.receiveResponse(response);
	assert (response.getContentLength() == HTTPMessage::UNKNOWN_CONTENT_LENGTH);
	assert (response.getContentType() == "text/plain");
	std::ostringstream ostr;
	StreamCopier::copyStream(rs, ostr);
	assert (ostr.str() == HTTPSTestServer::SMALL_BODY);
	assert ( dynamic_cast<const Poco::Net::SSLConnectionUnexpectedlyClosedException*>(
	         s.networkException()) != NULL );
}


void HTTPSClientSessionTest::setUp()
{
}


void HTTPSClientSessionTest::tearDown()
{
}


CppUnit::Test* HTTPSClientSessionTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("HTTPSClientSessionTest");

	CppUnit_addTest(pSuite, HTTPSClientSessionTest, testGetSmall);
	CppUnit_addTest(pSuite, HTTPSClientSessionTest, testGetLarge);
	CppUnit_addTest(pSuite, HTTPSClientSessionTest, testHead);
	CppUnit_addTest(pSuite, HTTPSClientSessionTest, testPostSmallIdentity);
	CppUnit_addTest(pSuite, HTTPSClientSessionTest, testPostLargeIdentity);
	CppUnit_addTest(pSuite, HTTPSClientSessionTest, testPostSmallChunked);
	CppUnit_addTest(pSuite, HTTPSClientSessionTest, testPostLargeChunked);
	CppUnit_addTest(pSuite, HTTPSClientSessionTest, testPostLargeChunkedKeepAlive);
	CppUnit_addTest(pSuite, HTTPSClientSessionTest, testKeepAlive);
	CppUnit_addTest(pSuite, HTTPSClientSessionTest, testInterop);
	CppUnit_addTest(pSuite, HTTPSClientSessionTest, testProxy);
	CppUnit_addTest(pSuite, HTTPSClientSessionTest, testCachedSession);
	CppUnit_addTest(pSuite, HTTPSClientSessionTest, testUnknownContentLength);
	CppUnit_addTest(pSuite, HTTPSClientSessionTest, testServerAbort);

	return pSuite;
}
