//
// HTTPResponseTest.cpp
//
// $Id: //poco/1.4/Net/testsuite/src/HTTPResponseTest.cpp#2 $
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "HTTPResponseTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Net/HTTPResponse.h"
#include "Poco/Net/HTTPCookie.h"
#include "Poco/Net/NetException.h"
#include <sstream>

GCC_DIAG_OFF(parentheses)

using Poco::Net::HTTPResponse;
using Poco::Net::HTTPMessage;
using Poco::Net::HTTPCookie;
using Poco::Net::MessageException;


HTTPResponseTest::HTTPResponseTest(const std::string& name): CppUnit::TestCase(name)
{
}


HTTPResponseTest::~HTTPResponseTest()
{
}


void HTTPResponseTest::testWrite1()
{
	HTTPResponse response;
	std::ostringstream ostr;
	response.write(ostr);
	std::string s = ostr.str();
	assert (s == "HTTP/1.0 200 OK\r\n\r\n");
}


void HTTPResponseTest::testWrite2()
{
	HTTPResponse response(HTTPMessage::HTTP_1_1, HTTPResponse::HTTP_MOVED_PERMANENTLY);
	response.set("Location", "http://www.appinf.com/index.html");
	response.set("Server", "Poco/1.0");
	std::ostringstream ostr;
	response.write(ostr);
	std::string s = ostr.str();
	assert (s == "HTTP/1.1 301 Moved Permanently\r\nLocation: http://www.appinf.com/index.html\r\nServer: Poco/1.0\r\n\r\n");
}


void HTTPResponseTest::testRead1()
{
	std::string s("HTTP/1.1 500 Internal Server Error\r\n\r\n");
	std::istringstream istr(s);
	HTTPResponse response;
	response.read(istr);
	assert (response.getStatus() == HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
	assert (response.getReason() == "Internal Server Error");
	assert (response.getVersion() == HTTPMessage::HTTP_1_1);
	assert (response.empty());
	assert (istr.get() == -1);
}


void HTTPResponseTest::testRead2()
{
	std::string s("HTTP/1.0 301 Moved Permanently\r\nLocation: http://www.appinf.com/index.html\r\nServer: Poco/1.0\r\n\r\n");
	std::istringstream istr(s);
	HTTPResponse response;
	response.read(istr);
	assert (response.getStatus() == HTTPResponse::HTTP_MOVED_PERMANENTLY);
	assert (response.getReason() == "Moved Permanently");
	assert (response.getVersion() == HTTPMessage::HTTP_1_0);
	assert (response.size() == 2);
	assert (response["Location"] == "http://www.appinf.com/index.html");
	assert (response["Server"] == "Poco/1.0");
	assert (istr.get() == -1);
}


void HTTPResponseTest::testRead3()
{
	std::string s("HTTP/1.1 200 \r\nContent-Length: 0\r\n\r\n");
	std::istringstream istr(s);
	HTTPResponse response;
	response.read(istr);
	assert (response.getVersion() == HTTPMessage::HTTP_1_1);
	assert (response.getStatus() == HTTPResponse::HTTP_OK);
	assert (response.getReason() == "");
	assert (response.size() == 1);
	assert (response.getContentLength() == 0);
	assert (istr.get() == -1);
}


void HTTPResponseTest::testInvalid1()
{
	std::string s(256, 'x');
	std::istringstream istr(s);
	HTTPResponse response;
	try
	{
		response.read(istr);
		fail("inavalid response - must throw");
	}
	catch (MessageException&)
	{
	}
}


void HTTPResponseTest::testInvalid2()
{
	std::string s("HTTP/1.1 200 ");
	s.append(1000, 'x');
	s.append("\r\n\r\n");
	std::istringstream istr(s);
	HTTPResponse response;
	try
	{
		response.read(istr);
		fail("inavalid response - must throw");
	}
	catch (MessageException&)
	{
	}
}


void HTTPResponseTest::testInvalid3()
{
	std::string s("HTTP/1.0 ");
	s.append(8000, 'x');
	s.append("\r\n\r\n");
	std::istringstream istr(s);
	HTTPResponse response;
	try
	{
		response.read(istr);
		fail("inavalid response - must throw");
	}
	catch (MessageException&)
	{
	}
}


void HTTPResponseTest::testCookies()
{
	HTTPResponse response;
	HTTPCookie cookie1("cookie1", "value1");
	response.addCookie(cookie1);
	std::vector<HTTPCookie> cookies;
	response.getCookies(cookies);
	assert (cookies.size() == 1);
	assert (cookie1.getVersion() == cookies[0].getVersion());
	assert (cookie1.getName() == cookies[0].getName());
	assert (cookie1.getValue() == cookies[0].getValue());
	assert (cookie1.getComment() == cookies[0].getComment());
	assert (cookie1.getDomain() == cookies[0].getDomain());
	assert (cookie1.getPath() == cookies[0].getPath());
	assert (cookie1.getSecure() == cookies[0].getSecure());
	assert (cookie1.getMaxAge() == cookies[0].getMaxAge());
	
	HTTPCookie cookie2("cookie2", "value2");
	cookie2.setVersion(1);
	cookie2.setMaxAge(42);
	cookie2.setSecure(true);
	response.addCookie(cookie2);
	response.getCookies(cookies);
	assert (cookies.size() == 2);
	HTTPCookie cookie2a;
	if (cookies[0].getName() == cookie2.getName())
		cookie2a = cookies[0];
	else
		cookie2a = cookies[1];
	assert (cookie2.getVersion() == cookie2a.getVersion());
	assert (cookie2.getName() == cookie2a.getName());
	assert (cookie2.getValue() == cookie2a.getValue());
	assert (cookie2.getComment() == cookie2a.getComment());
	assert (cookie2.getDomain() == cookie2a.getDomain());
	assert (cookie2.getPath() == cookie2a.getPath());
	assert (cookie2.getSecure() == cookie2a.getSecure());
	assert (cookie2.getMaxAge() == cookie2a.getMaxAge());
	
	HTTPResponse response2;
	response2.add("Set-Cookie", "name1=value1");
	response2.add("Set-cookie", "name2=value2");
	cookies.clear();
	response2.getCookies(cookies);
	assert (cookies.size() == 2);
	assert (cookies[0].getName() == "name1" && cookies[1].getName() == "name2" 
	     || cookies[0].getName() == "name2" && cookies[1].getName() == "name1"); 
}


void HTTPResponseTest::setUp()
{
}


void HTTPResponseTest::tearDown()
{
}


CppUnit::Test* HTTPResponseTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("HTTPResponseTest");

	CppUnit_addTest(pSuite, HTTPResponseTest, testWrite1);
	CppUnit_addTest(pSuite, HTTPResponseTest, testWrite2);
	CppUnit_addTest(pSuite, HTTPResponseTest, testRead1);
	CppUnit_addTest(pSuite, HTTPResponseTest, testRead2);
	CppUnit_addTest(pSuite, HTTPResponseTest, testRead3);
	CppUnit_addTest(pSuite, HTTPResponseTest, testInvalid1);
	CppUnit_addTest(pSuite, HTTPResponseTest, testInvalid2);
	CppUnit_addTest(pSuite, HTTPResponseTest, testInvalid3);
	CppUnit_addTest(pSuite, HTTPResponseTest, testCookies);

	return pSuite;
}
