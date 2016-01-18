//
// HTTPSServerTest.cpp
//
// $Id: //poco/1.4/NetSSL_OpenSSL/testsuite/src/HTTPSServerTest.cpp#1 $
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "HTTPSServerTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Net/HTTPServer.h"
#include "Poco/Net/HTTPServerParams.h"
#include "Poco/Net/HTTPRequestHandler.h"
#include "Poco/Net/HTTPRequestHandlerFactory.h"
#include "Poco/Net/HTTPSClientSession.h"
#include "Poco/Net/HTTPRequest.h"
#include "Poco/Net/HTTPServerRequest.h"
#include "Poco/Net/HTTPResponse.h"
#include "Poco/Net/HTTPServerResponse.h"
#include "Poco/Net/SecureServerSocket.h"
#include "Poco/StreamCopier.h"
#include <sstream>


using Poco::Net::HTTPServer;
using Poco::Net::HTTPServerParams;
using Poco::Net::HTTPRequestHandler;
using Poco::Net::HTTPRequestHandlerFactory;
using Poco::Net::HTTPSClientSession;
using Poco::Net::HTTPRequest;
using Poco::Net::HTTPServerRequest;
using Poco::Net::HTTPResponse;
using Poco::Net::HTTPServerResponse;
using Poco::Net::HTTPMessage;
using Poco::Net::SecureServerSocket;
using Poco::StreamCopier;


namespace
{
	class EchoBodyRequestHandler: public HTTPRequestHandler
	{
	public:
		void handleRequest(HTTPServerRequest& request, HTTPServerResponse& response)
		{
			if (request.getChunkedTransferEncoding())
				response.setChunkedTransferEncoding(true);
			else if (request.getContentLength() != HTTPMessage::UNKNOWN_CONTENT_LENGTH)
				response.setContentLength(request.getContentLength());
			
			response.setContentType(request.getContentType());
			
			std::istream& istr = request.stream();
			std::ostream& ostr = response.send();
			StreamCopier::copyStream(istr, ostr);
		}
	};
	
	class EchoHeaderRequestHandler: public HTTPRequestHandler
	{
	public:
		void handleRequest(HTTPServerRequest& request, HTTPServerResponse& response)
		{
			std::ostringstream osstr;
			request.write(osstr);
			int n = (int) osstr.str().length();
			response.setContentLength(n);
			std::ostream& ostr = response.send();
			if (request.getMethod() != HTTPRequest::HTTP_HEAD)
				request.write(ostr);
		}
	};

	class RedirectRequestHandler: public HTTPRequestHandler
	{
	public:
		void handleRequest(HTTPServerRequest& request, HTTPServerResponse& response)
		{
			response.redirect("http://www.appinf.com/");
		}
	};

	class AuthRequestHandler: public HTTPRequestHandler
	{
	public:
		void handleRequest(HTTPServerRequest& request, HTTPServerResponse& response)
		{
			response.requireAuthentication("/auth");
			response.send();
		}
	};
	
	class RequestHandlerFactory: public HTTPRequestHandlerFactory
	{
	public:
		HTTPRequestHandler* createRequestHandler(const HTTPServerRequest& request)
		{
			if (request.getURI() == "/echoBody")
				return new EchoBodyRequestHandler;
			else if (request.getURI() == "/echoHeader")
				return new EchoHeaderRequestHandler;
			else if (request.getURI() == "/redirect")
				return new RedirectRequestHandler();
			else if (request.getURI() == "/auth")
				return new AuthRequestHandler();
			else
				return 0;
		}
	};
}


HTTPSServerTest::HTTPSServerTest(const std::string& name): CppUnit::TestCase(name)
{
}


HTTPSServerTest::~HTTPSServerTest()
{
}


void HTTPSServerTest::testIdentityRequest()
{
	SecureServerSocket svs(0);
	HTTPServerParams* pParams = new HTTPServerParams;
	pParams->setKeepAlive(false);
	HTTPServer srv(new RequestHandlerFactory, svs, pParams);
	srv.start();
	
	HTTPSClientSession cs("localhost", svs.address().port());
	std::string body(5000, 'x');
	HTTPRequest request("POST", "/echoBody");
	request.setContentLength((int) body.length());
	request.setContentType("text/plain");
	cs.sendRequest(request) << body;
	HTTPResponse response;
	std::string rbody;
	cs.receiveResponse(response) >> rbody;
	assert (response.getContentLength() == body.size());
	assert (response.getContentType() == "text/plain");
	assert (rbody == body);
}


void HTTPSServerTest::testChunkedRequest()
{
	SecureServerSocket svs(0);
	HTTPServerParams* pParams = new HTTPServerParams;
	pParams->setKeepAlive(false);
	HTTPServer srv(new RequestHandlerFactory, svs, pParams);
	srv.start();
	
	HTTPSClientSession cs("localhost", svs.address().port());
	std::string body(5000, 'x');
	HTTPRequest request("POST", "/echoBody");
	request.setContentType("text/plain");
	request.setChunkedTransferEncoding(true);
	cs.sendRequest(request) << body;
	HTTPResponse response;
	std::string rbody;
	cs.receiveResponse(response) >> rbody;
	assert (response.getContentLength() == HTTPMessage::UNKNOWN_CONTENT_LENGTH);
	assert (response.getContentType() == "text/plain");
	assert (response.getChunkedTransferEncoding());
	assert (rbody == body);
}


void HTTPSServerTest::testIdentityRequestKeepAlive()
{
	SecureServerSocket svs(0);
	HTTPServerParams* pParams = new HTTPServerParams;
	pParams->setKeepAlive(true);
	HTTPServer srv(new RequestHandlerFactory, svs, pParams);
	srv.start();
	
	HTTPSClientSession cs("localhost", svs.address().port());
	cs.setKeepAlive(true);
	std::string body(5000, 'x');
	HTTPRequest request("POST", "/echoBody", HTTPMessage::HTTP_1_1);
	request.setContentLength((int) body.length());
	request.setContentType("text/plain");
	cs.sendRequest(request) << body;
	HTTPResponse response;
	std::string rbody;
	cs.receiveResponse(response) >> rbody;
	assert (response.getContentLength() == body.size());
	assert (response.getContentType() == "text/plain");
	assert (response.getKeepAlive());
	assert (rbody == body);
	
	body.assign(1000, 'y');
	request.setContentLength((int) body.length());
	request.setKeepAlive(false);
	cs.sendRequest(request) << body;
	cs.receiveResponse(response) >> rbody;
	assert (response.getContentLength() == body.size());
	assert (response.getContentType() == "text/plain");
	assert (!response.getKeepAlive());
	assert (rbody == body);}


void HTTPSServerTest::testChunkedRequestKeepAlive()
{
	SecureServerSocket svs(0);
	HTTPServerParams* pParams = new HTTPServerParams;
	pParams->setKeepAlive(true);
	HTTPServer srv(new RequestHandlerFactory, svs, pParams);
	srv.start();
	
	HTTPSClientSession cs("localhost", svs.address().port());
	cs.setKeepAlive(true);
	std::string body(5000, 'x');
	HTTPRequest request("POST", "/echoBody", HTTPMessage::HTTP_1_1);
	request.setContentType("text/plain");
	request.setChunkedTransferEncoding(true);
	cs.sendRequest(request) << body;
	HTTPResponse response;
	std::string rbody;
	cs.receiveResponse(response) >> rbody;
	assert (response.getContentLength() == HTTPMessage::UNKNOWN_CONTENT_LENGTH);
	assert (response.getContentType() == "text/plain");
	assert (response.getChunkedTransferEncoding());
	assert (rbody == body);

	body.assign(1000, 'y');
	request.setKeepAlive(false);
	cs.sendRequest(request) << body;
	cs.receiveResponse(response) >> rbody;
	assert (response.getContentLength() == HTTPMessage::UNKNOWN_CONTENT_LENGTH);
	assert (response.getContentType() == "text/plain");
	assert (response.getChunkedTransferEncoding());
	assert (!response.getKeepAlive());
	assert (rbody == body);
}


void HTTPSServerTest::test100Continue()
{
	SecureServerSocket svs(0);
	HTTPServerParams* pParams = new HTTPServerParams;
	pParams->setKeepAlive(false);
	HTTPServer srv(new RequestHandlerFactory, svs, pParams);
	srv.start();
	
	HTTPSClientSession cs("localhost", svs.address().port());
	std::string body(5000, 'x');
	HTTPRequest request("POST", "/echoBody");
	request.setContentLength((int) body.length());
	request.setContentType("text/plain");
	request.set("Expect", "100-Continue");
	cs.sendRequest(request) << body;
	HTTPResponse response;
	std::string rbody;
	cs.receiveResponse(response) >> rbody;
	assert (response.getContentLength() == body.size());
	assert (response.getContentType() == "text/plain");
	assert (rbody == body);
}


void HTTPSServerTest::testRedirect()
{
	SecureServerSocket svs(0);
	HTTPServerParams* pParams = new HTTPServerParams;
	pParams->setKeepAlive(false);
	HTTPServer srv(new RequestHandlerFactory, svs, pParams);
	srv.start();
	
	HTTPSClientSession cs("localhost", svs.address().port());
	HTTPRequest request("GET", "/redirect");
	cs.sendRequest(request);
	HTTPResponse response;
	std::string rbody;
	cs.receiveResponse(response) >> rbody;
	assert (response.getStatus() == HTTPResponse::HTTP_FOUND);
	assert (response.get("Location") == "http://www.appinf.com/");
	assert (rbody.empty());
}


void HTTPSServerTest::testAuth()
{
	SecureServerSocket svs(0);
	HTTPServerParams* pParams = new HTTPServerParams;
	pParams->setKeepAlive(false);
	HTTPServer srv(new RequestHandlerFactory, svs, pParams);
	srv.start();
	
	HTTPSClientSession cs("localhost", svs.address().port());
	HTTPRequest request("GET", "/auth");
	cs.sendRequest(request);
	HTTPResponse response;
	std::string rbody;
	cs.receiveResponse(response) >> rbody;
	assert (response.getStatus() == HTTPResponse::HTTP_UNAUTHORIZED);
	assert (response.get("WWW-Authenticate") == "Basic realm=\"/auth\"");
	assert (rbody.empty());
}


void HTTPSServerTest::testNotImpl()
{
	SecureServerSocket svs(0);
	HTTPServerParams* pParams = new HTTPServerParams;
	pParams->setKeepAlive(false);
	HTTPServer srv(new RequestHandlerFactory, svs, pParams);
	srv.start();
	
	HTTPSClientSession cs("localhost", svs.address().port());
	HTTPRequest request("GET", "/notImpl");
	cs.sendRequest(request);
	HTTPResponse response;
	std::string rbody;
	cs.receiveResponse(response) >> rbody;
	assert (response.getStatus() == HTTPResponse::HTTP_NOT_IMPLEMENTED);
	assert (rbody.empty());
}


void HTTPSServerTest::setUp()
{
}


void HTTPSServerTest::tearDown()
{
}


CppUnit::Test* HTTPSServerTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("HTTPSServerTest");

	CppUnit_addTest(pSuite, HTTPSServerTest, testIdentityRequest);
	CppUnit_addTest(pSuite, HTTPSServerTest, testChunkedRequest);
	CppUnit_addTest(pSuite, HTTPSServerTest, testIdentityRequestKeepAlive);
	CppUnit_addTest(pSuite, HTTPSServerTest, testChunkedRequestKeepAlive);
	CppUnit_addTest(pSuite, HTTPSServerTest, test100Continue);
	CppUnit_addTest(pSuite, HTTPSServerTest, testRedirect);
	CppUnit_addTest(pSuite, HTTPSServerTest, testAuth);
	CppUnit_addTest(pSuite, HTTPSServerTest, testNotImpl);

	return pSuite;
}
