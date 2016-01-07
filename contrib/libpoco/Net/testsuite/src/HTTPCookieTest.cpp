//
// HTTPCookieTest.cpp
//
// $Id: //poco/1.4/Net/testsuite/src/HTTPCookieTest.cpp#2 $
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "HTTPCookieTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Net/HTTPCookie.h"
#include "Poco/Timestamp.h"
#include "Poco/Timespan.h"
#include "Poco/DateTime.h"
#include "Poco/DateTimeFormatter.h"
#include "Poco/DateTimeParser.h"
#include "Poco/DateTimeFormat.h"
#include "Poco/Net/NameValueCollection.h"
#include <cstdlib>
#include <sstream>


using Poco::Timestamp;
using Poco::Timespan;
using Poco::DateTimeFormatter;
using Poco::DateTimeFormat;
using Poco::DateTimeParser;
using Poco::DateTime;
using Poco::Net::NameValueCollection;
using Poco::Net::HTTPCookie;


HTTPCookieTest::HTTPCookieTest(const std::string& name): CppUnit::TestCase(name)
{
}


HTTPCookieTest::~HTTPCookieTest()
{
}


void HTTPCookieTest::testCookie()
{
	HTTPCookie cookie("name", "value");
	assert (cookie.getName() == "name");
	assert (cookie.getValue() == "value");
	assert (cookie.toString() == "name=value");
	cookie.setPath("/");
	assert (cookie.toString() == "name=value; path=/");
	cookie.setComment("comment");
	assert (cookie.toString() == "name=value; path=/");
	cookie.setDomain("appinf.com");
	assert (cookie.toString() == "name=value; domain=appinf.com; path=/");
	cookie.setSecure(true);
	assert (cookie.toString() == "name=value; domain=appinf.com; path=/; secure");
	cookie.setHttpOnly(true);
	assert (cookie.toString() == "name=value; domain=appinf.com; path=/; secure; HttpOnly");
	cookie.setPriority("Low");
	assert (cookie.toString() == "name=value; domain=appinf.com; path=/; Priority=Low; secure; HttpOnly");
	cookie.setPriority("Medium");
	assert (cookie.toString() == "name=value; domain=appinf.com; path=/; Priority=Medium; secure; HttpOnly");
	cookie.setPriority("High");
	assert (cookie.toString() == "name=value; domain=appinf.com; path=/; Priority=High; secure; HttpOnly");
	cookie.setPriority("");
	cookie.setHttpOnly(false);

	cookie.setVersion(1);
	assert (cookie.toString() == "name=\"value\"; Comment=\"comment\"; Domain=\"appinf.com\"; Path=\"/\"; secure; Version=\"1\"");
	
	cookie.setSecure(false);
	cookie.setMaxAge(100);
	assert (cookie.toString() == "name=\"value\"; Comment=\"comment\"; Domain=\"appinf.com\"; Path=\"/\"; Max-Age=\"100\"; Version=\"1\"");
	
	cookie.setHttpOnly(true);
	assert (cookie.toString() == "name=\"value\"; Comment=\"comment\"; Domain=\"appinf.com\"; Path=\"/\"; Max-Age=\"100\"; HttpOnly; Version=\"1\"");

	cookie.setPriority("Low");
	assert (cookie.toString() == "name=\"value\"; Comment=\"comment\"; Domain=\"appinf.com\"; Path=\"/\"; Priority=\"Low\"; Max-Age=\"100\"; HttpOnly; Version=\"1\"");
	cookie.setPriority("Medium");
	assert (cookie.toString() == "name=\"value\"; Comment=\"comment\"; Domain=\"appinf.com\"; Path=\"/\"; Priority=\"Medium\"; Max-Age=\"100\"; HttpOnly; Version=\"1\"");
	cookie.setPriority("High");
	assert (cookie.toString() == "name=\"value\"; Comment=\"comment\"; Domain=\"appinf.com\"; Path=\"/\"; Priority=\"High\"; Max-Age=\"100\"; HttpOnly; Version=\"1\"");	
}


void HTTPCookieTest::testEscape()
{
	std::string escaped = HTTPCookie::escape("this is a test!");
	assert (escaped == "this%20is%20a%20test!");

	escaped = HTTPCookie::escape("\n\t@,;\"'");
	assert (escaped == "%0A%09@%2C%3B%22%27");
}


void HTTPCookieTest::testUnescape()
{
	std::string unescaped = HTTPCookie::unescape("this%20is%20a%20test!");
	assert (unescaped == "this is a test!");

	unescaped = HTTPCookie::unescape("%0a%09@%2c%3b%22%27");
	assert (unescaped == "\n\t@,;\"'");
}


void HTTPCookieTest::testExpiryFuture()
{
	DateTime future;
	//1 year from now
	future.assign(future.year() + 1,
		future.month(),
		future.day(),
		future.hour(),
		future.minute(),
		future.second(),
		future.millisecond(),
		future.microsecond());
	testCookieExpiry(future);
}


void HTTPCookieTest::testExpiryPast()
{
	DateTime past;
	// 1 year ago
	past.assign(past.year() - 1,
		past.month(),
		past.day(),
		past.hour(),
		past.minute(),
		past.second(),
		past.millisecond(),
		past.microsecond());
	testCookieExpiry(past);
}


void HTTPCookieTest::testCookieExpiry(DateTime expiryTime)
{
	NameValueCollection nvc;
	nvc.add("name", "value");
	std::string expiryString = DateTimeFormatter::format(expiryTime.timestamp(),DateTimeFormat::HTTP_FORMAT);
	nvc.add("expires", expiryString);

	Timestamp before; //start of cookie lifetime
	HTTPCookie cookie(nvc); //cookie created
	std::string cookieStringV0 = cookie.toString();
	cookie.setVersion(1);
	std::string cookieStringV1 = cookie.toString();
	Timestamp now;
	//expected number of seconds until expiryTime - should be close to cookie._maxAge
	int expectedMaxAge = (int) ((expiryTime.timestamp() - now) / Timestamp::resolution()); //expected number of seconds until expiryTime
	Timestamp after; //end of cookie lifetime

	//length of lifetime of the cookie
	Timespan delta = after - before;

	//pull out cookie expire time string
	size_t startPos = cookieStringV0.find("expires=") + 8;
	std::string cookieExpireTimeStr = cookieStringV0.substr(startPos, cookieStringV0.find(";", startPos));
	//convert to a DateTime
	int tzd;
	DateTime cookieExpireTime = DateTimeParser::parse(cookieExpireTimeStr, tzd);
	//pull out cookie max age
	int cookieMaxAge;
	startPos = cookieStringV1.find("Max-Age=\"") + 9;
	std::string cookieMaxAgeStr = cookieStringV1.substr(startPos, cookieStringV1.find("\"", startPos));
	//convert to integer
	std::istringstream(cookieMaxAgeStr) >> cookieMaxAge;

	//assert that the cookie's expiry time reflects the time passed to
	//its constructor, within a delta of the lifetime of the cookie
	assert (cookieExpireTime - expiryTime <= delta);
	//assert that the cookie's max age is the number of seconds between
	//the creation of the cookie and the expiry time passed to its
	//constuctor, within a delta of the lifetime of the cookie
	assert (cookieMaxAge - expectedMaxAge <= delta.seconds());
}


void HTTPCookieTest::setUp()
{
}


void HTTPCookieTest::tearDown()
{
}


CppUnit::Test* HTTPCookieTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("HTTPCookieTest");

	CppUnit_addTest(pSuite, HTTPCookieTest, testCookie);
	CppUnit_addTest(pSuite, HTTPCookieTest, testEscape);
	CppUnit_addTest(pSuite, HTTPCookieTest, testUnescape);
	CppUnit_addTest(pSuite, HTTPCookieTest, testExpiryFuture);
	CppUnit_addTest(pSuite, HTTPCookieTest, testExpiryPast);

	return pSuite;
}
