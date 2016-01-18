//
// URITest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/URITest.cpp#3 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "URITest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/URI.h"
#include "Poco/Path.h"


using Poco::URI;
using Poco::Path;


URITest::URITest(const std::string& name): CppUnit::TestCase(name)
{
}


URITest::~URITest()
{
}


void URITest::testConstruction()
{
	URI uri;
	assert (uri.getScheme().empty());
	assert (uri.getAuthority().empty());
	assert (uri.getUserInfo().empty());
	assert (uri.getHost().empty());
	assert (uri.getPort() == 0);
	assert (uri.getPath().empty());
	assert (uri.getQuery().empty());
	assert (uri.getFragment().empty());
	
	uri.setScheme("ftp");
	assert (uri.getScheme() == "ftp");
	assert (uri.getPort() == 21);
	
	uri.setScheme("HTTP");
	assert (uri.getScheme() == "http");
	
	uri.setAuthority("www.appinf.com");
	assert (uri.getAuthority() == "www.appinf.com");
	assert (uri.getPort() == 80);
	
	uri.setAuthority("user@services.appinf.com:8000");
	assert (uri.getUserInfo() == "user");
	assert (uri.getHost() == "services.appinf.com");
	assert (uri.getPort() == 8000);
	
	uri.setPath("/index.html");
	assert (uri.getPath() == "/index.html");
	
	uri.setPath("/file%20with%20spaces.html");
	assert (uri.getPath() == "/file with spaces.html");
	
	uri.setPathEtc("/query.cgi?query=foo");
	assert (uri.getPath() == "/query.cgi");
	assert (uri.getQuery() == "query=foo");
	assert (uri.getFragment().empty());
	assert (uri.getPathEtc() == "/query.cgi?query=foo");
	assert (uri.getPathAndQuery() == "/query.cgi?query=foo");
	
	uri.setPathEtc("/query.cgi?query=bar#frag");
	assert (uri.getPath() == "/query.cgi");
	assert (uri.getQuery() == "query=bar");
	assert (uri.getFragment() == "frag");
	assert (uri.getPathEtc() == "/query.cgi?query=bar#frag");
	assert (uri.getPathAndQuery() == "/query.cgi?query=bar");
	
	uri.setQuery("query=test");
	assert (uri.getQuery() == "query=test");
	
	uri.setFragment("result");
	assert (uri.getFragment() == "result");
	
	URI uri2("file", "/home/guenter/foo.bar");
	assert (uri2.getScheme() == "file");
	assert (uri2.getPath() == "/home/guenter/foo.bar");
	
	URI uri3("http", "www.appinf.com", "/index.html");
	assert (uri3.getScheme() == "http");
	assert (uri3.getAuthority() == "www.appinf.com");
	assert (uri3.getPath() == "/index.html");
	
	URI uri4("http", "www.appinf.com:8000", "/index.html");
	assert (uri4.getScheme() == "http");
	assert (uri4.getAuthority() == "www.appinf.com:8000");
	assert (uri4.getPath() == "/index.html");

	URI uri5("http", "user@www.appinf.com:8000", "/index.html");
	assert (uri5.getScheme() == "http");
	assert (uri5.getUserInfo() == "user");
	assert (uri5.getHost() == "www.appinf.com");
	assert (uri5.getPort() == 8000);
	assert (uri5.getAuthority() == "user@www.appinf.com:8000");
	assert (uri5.getPath() == "/index.html");

	URI uri6("http", "user@www.appinf.com:80", "/index.html");
	assert (uri6.getScheme() == "http");
	assert (uri6.getUserInfo() == "user");
	assert (uri6.getHost() == "www.appinf.com");
	assert (uri6.getPort() == 80);
	assert (uri6.getAuthority() == "user@www.appinf.com");
	assert (uri6.getPath() == "/index.html");

	URI uri7("http", "user@www.appinf.com:", "/index.html");
	assert (uri7.getScheme() == "http");
	assert (uri7.getUserInfo() == "user");
	assert (uri7.getHost() == "www.appinf.com");
	assert (uri7.getPort() == 80);
	assert (uri7.getAuthority() == "user@www.appinf.com");
	assert (uri7.getPath() == "/index.html");
	
	URI uri8("http", "www.appinf.com", "/index.html", "query=test");
	assert (uri8.getScheme() == "http");
	assert (uri8.getAuthority() == "www.appinf.com");
	assert (uri8.getPath() == "/index.html");
	assert (uri8.getQuery() == "query=test");

	URI uri9("http", "www.appinf.com", "/index.html", "query=test", "fragment");
	assert (uri9.getScheme() == "http");
	assert (uri9.getAuthority() == "www.appinf.com");
	assert (uri9.getPath() == "/index.html");
	assert (uri9.getPathEtc() == "/index.html?query=test#fragment");
	assert (uri9.getQuery() == "query=test");
	assert (uri9.getFragment() == "fragment");

	uri9.clear();
	assert (uri9.getScheme().empty());
	assert (uri9.getAuthority().empty());
	assert (uri9.getUserInfo().empty());
	assert (uri9.getHost().empty());
	assert (uri9.getPort() == 0);
	assert (uri9.getPath().empty());
	assert (uri9.getQuery().empty());
	assert (uri9.getFragment().empty());

	URI uri10("ldap", "[2001:db8::7]", "/c=GB?objectClass?one");
	assert (uri10.getScheme() == "ldap");
	assert (uri10.getUserInfo().empty());
	assert (uri10.getHost() == "2001:db8::7");
	assert (uri10.getPort() == 389);
	assert (uri10.getAuthority() == "[2001:db8::7]");
	assert (uri10.getPathEtc() == "/c=GB?objectClass?one");
	
	URI uri11("http", "www.appinf.com", "/index.html?query=test#fragment");
	assert (uri11.getScheme() == "http");
	assert (uri11.getAuthority() == "www.appinf.com");
	assert (uri11.getPath() == "/index.html");
	assert (uri11.getPathEtc() == "/index.html?query=test#fragment");
	assert (uri11.getQuery() == "query=test");
	assert (uri11.getFragment() == "fragment");
}


void URITest::testParse()
{
	URI uri("http://www.appinf.com");
	assert (uri.getScheme() == "http");
	assert (uri.getAuthority() == "www.appinf.com");
	assert (uri.getPath().empty());
	assert (uri.getQuery().empty());
	assert (uri.getFragment().empty());
	assert (!uri.isRelative());

	uri = "http://www.appinf.com/";
	assert (uri.getScheme() == "http");
	assert (uri.getAuthority() == "www.appinf.com");
	assert (uri.getPath() == "/");
	assert (uri.getQuery().empty());
	assert (uri.getFragment().empty());
	assert (!uri.isRelative());
	
	uri = "ftp://anonymous@ftp.appinf.com/pub/";
	assert (uri.getScheme() == "ftp");
	assert (uri.getUserInfo() == "anonymous");
	assert (uri.getHost() == "ftp.appinf.com");
	assert (uri.getPort() == 21);
	assert (uri.getAuthority() == "anonymous@ftp.appinf.com");
	assert (uri.getPath() == "/pub/");
	assert (uri.getQuery().empty());
	assert (uri.getFragment().empty());
	assert (!uri.isRelative());
	assert (!uri.isRelative());

	uri = "https://www.appinf.com/index.html#top";
	assert (uri.getScheme() == "https");
	assert (uri.getHost() == "www.appinf.com");
	assert (uri.getPort() == 443);
	assert (uri.getPath() == "/index.html");
	assert (uri.getQuery().empty());
	assert (uri.getFragment() == "top");
	assert (!uri.isRelative());
	
	uri = "http://www.appinf.com/search.cgi?keyword=test&scope=all";
	assert (uri.getScheme() == "http");
	assert (uri.getHost() == "www.appinf.com");
	assert (uri.getPort() == 80);
	assert (uri.getPath() == "/search.cgi");
	assert (uri.getQuery() == "keyword=test&scope=all");
	assert (uri.getFragment().empty());
	assert (!uri.isRelative());

	uri = "http://www.appinf.com/search.cgi?keyword=test&scope=all#result";
	assert (uri.getScheme() == "http");
	assert (uri.getHost() == "www.appinf.com");
	assert (uri.getPort() == 80);
	assert (uri.getPath() == "/search.cgi");
	assert (uri.getQuery() == "keyword=test&scope=all");
	assert (uri.getFragment() == "result");
	assert (!uri.isRelative());
	
	uri = "http://www.appinf.com/search.cgi?keyword=test%20encoded&scope=all#result";
	assert (uri.getScheme() == "http");
	assert (uri.getHost() == "www.appinf.com");
	assert (uri.getPort() == 80);
	assert (uri.getPath() == "/search.cgi");
	assert (uri.getQuery() == "keyword=test encoded&scope=all");
	assert (uri.getFragment() == "result");
	assert (!uri.isRelative());
	
	uri = "ldap://[2001:db8::7]/c=GB?objectClass?one";
	assert (uri.getScheme() == "ldap");
	assert (uri.getUserInfo().empty());
	assert (uri.getHost() == "2001:db8::7");
	assert (uri.getPort() == 389);
	assert (uri.getAuthority() == "[2001:db8::7]");
	assert (uri.getPath() == "/c=GB");
	assert (uri.getQuery() == "objectClass?one");
	assert (uri.getFragment().empty());
	
	uri = "mailto:John.Doe@example.com";
	assert (uri.getScheme() == "mailto");
	assert (uri.getUserInfo().empty());
	assert (uri.getHost().empty());
	assert (uri.getPort() == 0);
	assert (uri.getAuthority().empty());
	assert (uri.getPath() == "John.Doe@example.com");
	assert (uri.getQuery().empty());
	assert (uri.getFragment().empty());
	
	uri = "tel:+1-816-555-1212";
	assert (uri.getScheme() == "tel");
	assert (uri.getUserInfo().empty());
	assert (uri.getHost().empty());
	assert (uri.getPort() == 0);
	assert (uri.getAuthority().empty());
	assert (uri.getPath() == "+1-816-555-1212");
	assert (uri.getQuery().empty());
	assert (uri.getFragment().empty());
	
	uri = "telnet://192.0.2.16:80";
	assert (uri.getScheme() == "telnet");
	assert (uri.getUserInfo().empty());
	assert (uri.getHost() == "192.0.2.16");
	assert (uri.getPort() == 80);
	assert (uri.getAuthority() == "192.0.2.16:80");
	assert (uri.getPath().empty());
	assert (uri.getQuery().empty());
	assert (uri.getFragment().empty());
	
	uri = "urn:oasis:names:specification:docbook:dtd:xml:4.1.2";
	assert (uri.getScheme() == "urn");
	assert (uri.getUserInfo().empty());
	assert (uri.getHost().empty());
	assert (uri.getPort() == 0);
	assert (uri.getAuthority().empty());
	assert (uri.getPath() == "oasis:names:specification:docbook:dtd:xml:4.1.2");
	assert (uri.getQuery().empty());
	assert (uri.getFragment().empty());
	
	uri = "";
	assert (uri.getScheme().empty());
	assert (uri.getAuthority().empty());
	assert (uri.getUserInfo().empty());
	assert (uri.getHost().empty());
	assert (uri.getPort() == 0);
	assert (uri.getPath().empty());
	assert (uri.getQuery().empty());
	assert (uri.getFragment().empty());
	assert (uri.empty());
	
	// relative references
	
	uri = "/foo/bar";
	assert (uri.getScheme().empty());
	assert (uri.getAuthority().empty());
	assert (uri.getUserInfo().empty());
	assert (uri.getHost().empty());
	assert (uri.getPort() == 0);
	assert (uri.getPath() == "/foo/bar");
	assert (uri.getQuery().empty());
	assert (uri.getFragment().empty());
	assert (uri.isRelative());

	uri = "./foo/bar";
	assert (uri.getScheme().empty());
	assert (uri.getAuthority().empty());
	assert (uri.getUserInfo().empty());
	assert (uri.getHost().empty());
	assert (uri.getPort() == 0);
	assert (uri.getPath() == "./foo/bar");
	assert (uri.getQuery().empty());
	assert (uri.getFragment().empty());
	assert (uri.isRelative());

	uri = "../foo/bar";
	assert (uri.getScheme().empty());
	assert (uri.getAuthority().empty());
	assert (uri.getUserInfo().empty());
	assert (uri.getHost().empty());
	assert (uri.getPort() == 0);
	assert (uri.getPath() == "../foo/bar");
	assert (uri.getQuery().empty());
	assert (uri.getFragment().empty());
	assert (uri.isRelative());

	uri = "index.html";
	assert (uri.getScheme().empty());
	assert (uri.getAuthority().empty());
	assert (uri.getUserInfo().empty());
	assert (uri.getHost().empty());
	assert (uri.getPort() == 0);
	assert (uri.getPath() == "index.html");
	assert (uri.getQuery().empty());
	assert (uri.getFragment().empty());
	assert (uri.isRelative());

	uri = "index.html#frag";
	assert (uri.getScheme().empty());
	assert (uri.getAuthority().empty());
	assert (uri.getUserInfo().empty());
	assert (uri.getHost().empty());
	assert (uri.getPort() == 0);
	assert (uri.getPath() == "index.html");
	assert (uri.getQuery().empty());
	assert (uri.getFragment() == "frag");
	assert (uri.isRelative());
	
	uri = "?query=test";	
	assert (uri.getScheme().empty());
	assert (uri.getAuthority().empty());
	assert (uri.getUserInfo().empty());
	assert (uri.getHost().empty());
	assert (uri.getPort() == 0);
	assert (uri.getPath().empty());
	assert (uri.getQuery() == "query=test");
	assert (uri.getFragment().empty());
	assert (uri.isRelative());

	uri = "?query=test#frag";	
	assert (uri.getScheme().empty());
	assert (uri.getAuthority().empty());
	assert (uri.getUserInfo().empty());
	assert (uri.getHost().empty());
	assert (uri.getPort() == 0);
	assert (uri.getPath().empty());
	assert (uri.getQuery() == "query=test");
	assert (uri.getFragment() == "frag");
	assert (uri.isRelative());
	
	uri = "#frag";	
	assert (uri.getScheme().empty());
	assert (uri.getAuthority().empty());
	assert (uri.getUserInfo().empty());
	assert (uri.getHost().empty());
	assert (uri.getPort() == 0);
	assert (uri.getPath().empty());
	assert (uri.getQuery().empty());
	assert (uri.getFragment() == "frag");
	assert (uri.isRelative());

	uri = "#";	
	assert (uri.getScheme().empty());
	assert (uri.getAuthority().empty());
	assert (uri.getUserInfo().empty());
	assert (uri.getHost().empty());
	assert (uri.getPort() == 0);
	assert (uri.getPath().empty());
	assert (uri.getQuery().empty());
	assert (uri.getFragment().empty());
	assert (uri.isRelative());
	
	uri = "file:///a/b/c";
	assert (uri.getScheme() == "file");
	assert (uri.getAuthority().empty());
	assert (uri.getUserInfo().empty());
	assert (uri.getHost().empty());
	assert (uri.getPort() == 0);
	assert (uri.getPath() == "/a/b/c");
	assert (uri.getQuery().empty());
	assert (uri.getFragment().empty());
	assert (!uri.isRelative());

	uri = "file://localhost/a/b/c";
	assert (uri.getScheme() == "file");
	assert (uri.getAuthority() == "localhost");
	assert (uri.getUserInfo().empty());
	assert (uri.getHost() == "localhost");
	assert (uri.getPort() == 0);
	assert (uri.getPath() == "/a/b/c");
	assert (uri.getQuery().empty());
	assert (uri.getFragment().empty());
	assert (!uri.isRelative());
	
	uri = "file:///c:/Windows/system32/";
	assert (uri.getScheme() == "file");
	assert (uri.getAuthority().empty());
	assert (uri.getUserInfo().empty());
	assert (uri.getHost().empty());
	assert (uri.getPort() == 0);
	assert (uri.getPath() == "/c:/Windows/system32/");
	assert (uri.getQuery().empty());
	assert (uri.getFragment().empty());
	assert (!uri.isRelative());

	uri = "./c:/Windows/system32/";
	assert (uri.getScheme().empty());
	assert (uri.getAuthority().empty());
	assert (uri.getUserInfo().empty());
	assert (uri.getHost().empty());
	assert (uri.getPort() == 0);
	assert (uri.getPath() == "./c:/Windows/system32/");
	assert (uri.getQuery().empty());
	assert (uri.getFragment().empty());
	assert (uri.isRelative());
	
}


void URITest::testToString()
{
	URI uri("http://www.appinf.com");
	assert (uri.toString() == "http://www.appinf.com");

	uri = "http://www.appinf.com/";
	assert (uri.toString() == "http://www.appinf.com/");
	
	uri = "ftp://anonymous@ftp.appinf.com/pub/";
	assert (uri.toString() == "ftp://anonymous@ftp.appinf.com/pub/");

	uri = "https://www.appinf.com/index.html#top";
	assert (uri.toString() == "https://www.appinf.com/index.html#top");
	
	uri = "http://www.appinf.com/search.cgi?keyword=test&scope=all";
	assert (uri.toString() == "http://www.appinf.com/search.cgi?keyword=test&scope=all");

	uri = "http://www.appinf.com/search.cgi?keyword=test&scope=all#result";
	assert (uri.toString() == "http://www.appinf.com/search.cgi?keyword=test&scope=all#result");
	
	uri = "http://www.appinf.com/search.cgi?keyword=test%20encoded&scope=all#result";
	assert (uri.toString() == "http://www.appinf.com/search.cgi?keyword=test%20encoded&scope=all#result");
	
	uri = "ldap://[2001:db8::7]/c=GB?objectClass?one";
	assert (uri.toString() == "ldap://[2001:db8::7]/c=GB?objectClass?one");
	
	uri = "mailto:John.Doe@example.com";
	assert (uri.toString() == "mailto:John.Doe@example.com");
	
	uri = "tel:+1-816-555-1212";
	assert (uri.toString() == "tel:+1-816-555-1212");
	
	uri = "telnet://192.0.2.16:80";
	assert (uri.toString() == "telnet://192.0.2.16:80");
	
	uri = "urn:oasis:names:specification:docbook:dtd:xml:4.1.2";
	assert (uri.toString() == "urn:oasis:names:specification:docbook:dtd:xml:4.1.2");
	
	uri = "";
	assert (uri.toString() == "");

	// relative references
	
	uri = "/foo/bar";
	assert (uri.toString() == "/foo/bar");

	uri = "./foo/bar";
	assert (uri.toString() == "./foo/bar");

	uri = "../foo/bar";
	assert (uri.toString() == "../foo/bar");

	uri = "//foo/bar";
	assert (uri.toString() == "//foo/bar");

	uri = "index.html";
	assert (uri.toString() == "index.html");

	uri = "index.html#frag";
	assert (uri.toString() == "index.html#frag");
	
	uri = "?query=test";	
	assert (uri.toString() == "?query=test");

	uri = "?query=test#frag";	
	assert (uri.toString() == "?query=test#frag");
	
	uri = "#frag";	
	assert (uri.toString() == "#frag");

	uri = "#";	
	assert (uri.toString() == "");
	
	uri = "file:///a/b/c";
	assert (uri.toString() == "file:///a/b/c");
	
	uri = "file://localhost/a/b/c";
	assert (uri.toString() == "file://localhost/a/b/c");
	
	uri = "file:///c:/Windows/system32/";
	assert (uri.toString() == "file:///c:/Windows/system32/");

	uri = "./c:/Windows/system32/";
	assert (uri.toString() == "./c:/Windows/system32/");
	
	uri = "http://www.appinf.com";
	uri.setRawQuery("query=test");
	assert (uri.toString() == "http://www.appinf.com/?query=test");
}


void URITest::testCompare()
{
	URI uri1("http://www.appinf.com");
	URI uri2("HTTP://www.appinf.com:80");
	assert (uri1 == uri2);
	assert (uri1 == "http://www.appinf.com:");
	assert (uri1 != "http://www.google.com");
	
	uri1 = "/foo/bar";
	assert (uri1 == "/foo/bar");
	assert (uri1 != "/foo/baz");
	
	uri1 = "?query";
	assert (uri1 == "?query");
	assert (uri1 != "?query2");
	
	uri1 = "#frag";
	assert (uri1 == "#frag");
	assert (uri1 != "#frag2");
	
	uri1 = "/index.html#frag";
	assert (uri1 == "/index.html#frag");
	assert (uri1 != "/index.html");
}


void URITest::testNormalize()
{
	URI uri("http://www.appinf.com");
	uri.normalize();
	assert (uri.toString() == "http://www.appinf.com");
	
	uri = "http://www.appinf.com/";
	uri.normalize();
	assert (uri.toString() == "http://www.appinf.com/");
	
	uri = "http://www.appinf.com/foo/bar/./index.html";
	uri.normalize();
	assert (uri.toString() == "http://www.appinf.com/foo/bar/index.html");
	
	uri = "http://www.appinf.com/foo/bar/../index.html";
	uri.normalize();
	assert (uri.toString() == "http://www.appinf.com/foo/index.html");

	uri = "http://www.appinf.com/foo/./bar/../index.html";
	uri.normalize();
	assert (uri.toString() == "http://www.appinf.com/foo/index.html");

	uri = "http://www.appinf.com/foo/./bar/../index.html";
	uri.normalize();
	assert (uri.toString() == "http://www.appinf.com/foo/index.html");

	uri = "http://www.appinf.com/foo/bar/../../index.html";
	uri.normalize();
	assert (uri.toString() == "http://www.appinf.com/index.html");

	uri = "http://www.appinf.com/foo/bar/../../../index.html";
	uri.normalize();
	assert (uri.toString() == "http://www.appinf.com/index.html");

	uri = "http://www.appinf.com/foo/bar/.././../index.html";
	uri.normalize();
	assert (uri.toString() == "http://www.appinf.com/index.html");

	uri = "http://www.appinf.com/./foo/bar/index.html";
	uri.normalize();
	assert (uri.toString() == "http://www.appinf.com/foo/bar/index.html");

	uri = "http://www.appinf.com/../foo/bar/index.html";
	uri.normalize();
	assert (uri.toString() == "http://www.appinf.com/foo/bar/index.html");

	uri = "http://www.appinf.com/../foo/bar/";
	uri.normalize();
	assert (uri.toString() == "http://www.appinf.com/foo/bar/");

	uri = "http://www.appinf.com/../foo/../";
	uri.normalize();
	assert (uri.toString() == "http://www.appinf.com/");
	
	uri = "file:///c:/Windows/system32/";
	uri.normalize();
	assert (uri.toString() == "file:///c:/Windows/system32/");

	uri.clear();
	uri.setPath("c:/windows/system32/");
	uri.normalize();
	assert (uri.toString() == "./c:/windows/system32/");
}


void URITest::testResolve()
{
	URI uri("http://www.appinf.com");
	
	uri.resolve("/index.html");
	assert (uri.toString() == "http://www.appinf.com/index.html");
	
	uri.resolve("#frag");
	assert (uri.toString() == "http://www.appinf.com/index.html#frag");
	
	uri = "http://www.appinf.com/html";
	uri.resolve("../images/foo.gif");
	assert (uri.toString() == "http://www.appinf.com/images/foo.gif");

	uri = "http://www.appinf.com/html/";
	uri.resolve(".");
	assert (uri.toString() == "http://www.appinf.com/html/");

	uri = "http://www.appinf.com/html/";
	uri.resolve(".");
	assert (uri.toString() == "http://www.appinf.com/html/");

	uri = "http://www.appinf.com/html/";
	uri.resolve("..");
	assert (uri.toString() == "http://www.appinf.com/");

	uri = "http://www.appinf.com/html/";
	uri.resolve("index.html");
	assert (uri.toString() == "http://www.appinf.com/html/index.html");

	uri = "http://www.appinf.com/html/";
	uri.resolve("/index.html");
	assert (uri.toString() == "http://www.appinf.com/index.html");

	uri = "/a/b/c/d/e";
	uri.resolve("./../../f/./g");
	assert (uri.toString() == "/a/b/f/g");
	
	uri = "/a/b/../c/";
	uri.resolve("../d");
	assert (uri.toString() == "/a/d");

	uri = "/a/b/../c/";
	uri.resolve("../d/");
	assert (uri.toString() == "/a/d/");

	uri = "/a/b/c/";
	uri.resolve("../../../../d/");
	assert (uri.toString() == "/d/");

	uri = "/a/b/c/";
	uri.resolve("%2e%2e/%2e%2e/%2e%2e/../d/");
	assert (uri.toString() == "/d/");

	uri = "/a/b/c/";
	uri.resolve("");
	assert (uri.toString() == "/a/b/c/");

	uri = "/a/b/c/";
	uri.resolve("/d/");
	assert (uri.toString() == "/d/");

	uri = "/a/b/c";
	uri.resolve("");
	assert (uri.toString() == "/a/b/c");

	uri = "/a/b/c";
	uri.resolve("?query=test");
	assert (uri.toString() == "/a/b/c?query=test");

	uri = "/a/b/c";
	uri.resolve("#frag");
	assert (uri.toString() == "/a/b/c#frag");

	uri = "http://www.appinf.com/html/";
	uri.resolve("http://www.google.com/");
	assert (uri.toString() == "http://www.google.com/");
	
	uri = "http://www.appinf.com/";
	URI uri2(uri, "index.html");
	assert (uri2.toString() == "http://www.appinf.com/index.html");

	uri = "index.html";
	URI uri3(uri, "search.html");
	assert (uri3.toString() == "search.html");
}


void URITest::testSwap()
{
	URI uri1("http://www.appinf.com/search.cgi?keyword=test%20encoded&scope=all#result");
	URI uri2("mailto:John.Doe@example.com");
	
	uri1.swap(uri2);
	assert (uri1.toString() == "mailto:John.Doe@example.com");
	assert (uri2.toString() == "http://www.appinf.com/search.cgi?keyword=test%20encoded&scope=all#result");
}


void URITest::testOther()
{
	// The search string is "hello%world"; google happens to ignore the '%'
	// character, so it finds lots of hits for "hello world" programs. This is
	// a convenient reproduction case, and a URL that actually works.
	Poco::URI uri("http://google.com/search?q=hello%25world#frag%20ment");

	assert(uri.getHost() == "google.com");
	assert(uri.getPath() == "/search");
	assert(uri.getQuery() == "q=hello%world");
	assert(uri.getRawQuery() == "q=hello%25world");
	assert(uri.getFragment() == "frag ment");
	assert(uri.toString() == "http://google.com/search?q=hello%25world#frag%20ment");
	assert(uri.getPathEtc() == "/search?q=hello%25world#frag%20ment");

	uri.setQuery("q=foo&bar");
	assert(uri.getQuery() == "q=foo&bar");
	assert(uri.getRawQuery() == "q=foo&bar");
	assert(uri.toString() == "http://google.com/search?q=foo&bar#frag%20ment");
	assert(uri.getPathEtc() == "/search?q=foo&bar#frag%20ment");

	uri.setQuery("q=foo/bar");
	assert(uri.getQuery() == "q=foo/bar");
	assert(uri.getRawQuery() == "q=foo%2Fbar");
	assert(uri.toString() == "http://google.com/search?q=foo%2Fbar#frag%20ment");
	assert(uri.getPathEtc() == "/search?q=foo%2Fbar#frag%20ment");

	uri.setQuery("q=goodbye cruel world");
	assert(uri.getQuery() == "q=goodbye cruel world");
	assert(uri.getRawQuery() == "q=goodbye%20cruel%20world");
	assert(uri.toString() == "http://google.com/search?q=goodbye%20cruel%20world#frag%20ment");
	assert(uri.getPathEtc() == "/search?q=goodbye%20cruel%20world#frag%20ment");

	uri.setRawQuery("q=pony%7eride");
	assert(uri.getQuery() == "q=pony~ride");
	assert(uri.getRawQuery() == "q=pony%7eride");
	assert(uri.toString() == "http://google.com/search?q=pony%7eride#frag%20ment");
	assert(uri.getPathEtc() == "/search?q=pony%7eride#frag%20ment");

	uri.addQueryParameter("pa=ra&m1");
	assert(uri.getRawQuery() == "q=pony%7eride&pa%3Dra%26m1=");
	assert(uri.getQuery() == "q=pony~ride&pa=ra&m1=");
	uri.addQueryParameter("pa=ra&m2", "val&ue");
	assert(uri.getRawQuery() == "q=pony%7eride&pa%3Dra%26m1=&pa%3Dra%26m2=val%26ue");
	assert(uri.getQuery() == "q=pony~ride&pa=ra&m1=&pa=ra&m2=val&ue");

	uri = "http://google.com/search?q=hello+world#frag%20ment";
	assert(uri.getHost() == "google.com");
	assert(uri.getPath() == "/search");
	assert(uri.getQuery() == "q=hello+world");
	assert(uri.getRawQuery() == "q=hello+world");
	assert(uri.getFragment() == "frag ment");
	assert(uri.toString() == "http://google.com/search?q=hello+world#frag%20ment");
	assert(uri.getPathEtc() == "/search?q=hello+world#frag%20ment");
}


void URITest::testEncodeDecode()
{
	std::string str;
	URI::encode("http://google.com/search?q=hello+world#frag ment", "+#?", str);
	assert (str == "http://google.com/search%3Fq=hello%2Bworld%23frag%20ment");

	str = "";
	URI::encode("http://google.com/search?q=hello+world#frag ment", "", str);
	assert (str == "http://google.com/search?q=hello+world#frag%20ment");
	
	str = "";
	URI::decode("http://google.com/search?q=hello+world#frag%20ment", str, true);
	assert (str == "http://google.com/search?q=hello world#frag ment");
	
	str = "";
	URI::decode("http://google.com/search?q=hello%2Bworld#frag%20ment", str);
	assert (str == "http://google.com/search?q=hello+world#frag ment");
}


void URITest::testFromPath()
{
	Path path1("/var/www/site/index.html", Path::PATH_UNIX);
	URI uri1(path1);
	assert (uri1.toString() == "file:///var/www/site/index.html");

	Path path2("/var/www/site/with space.html", Path::PATH_UNIX);
	URI uri2(path2);
	assert (uri2.toString() == "file:///var/www/site/with%20space.html");
	
	Path path3("c:\\www\\index.html", Path::PATH_WINDOWS);
	URI uri3(path3);
	assert (uri3.toString() == "file:///c:/www/index.html");
}


void URITest::testQueryParameters()
{
	Poco::URI uri("http://google.com/search?q=hello+world&client=safari");
	URI::QueryParameters params = uri.getQueryParameters();
	assert (params.size() == 2);
	assert (params[0].first == "q");
	assert (params[0].second == "hello world");
	assert (params[1].first == "client");
	assert (params[1].second == "safari");

	uri.setQueryParameters(params);
	assert (uri.toString() == "http://google.com/search?q=hello%20world&client=safari");

	uri = "http://google.com/search?q=&client&";
	params = uri.getQueryParameters();
	assert (params.size() == 2);
	assert (params[0].first == "q");
	assert (params[0].second == "");
	assert (params[1].first == "client");
	assert (params[1].second == "");

	uri.setQueryParameters(params);
	assert (uri.toString() == "http://google.com/search?q=&client=");

	params[0].second = "foo/bar?";
	uri.setQueryParameters(params);
	assert (uri.toString() == "http://google.com/search?q=foo%2Fbar%3F&client=");

	params[0].second = "foo&bar";
	uri.setQueryParameters(params);
	assert (uri.toString() == "http://google.com/search?q=foo%26bar&client=");
}


void URITest::setUp()
{
}


void URITest::tearDown()
{
}


CppUnit::Test* URITest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("URITest");

	CppUnit_addTest(pSuite, URITest, testConstruction);
	CppUnit_addTest(pSuite, URITest, testParse);
	CppUnit_addTest(pSuite, URITest, testToString);
	CppUnit_addTest(pSuite, URITest, testCompare);
	CppUnit_addTest(pSuite, URITest, testNormalize);
	CppUnit_addTest(pSuite, URITest, testResolve);
	CppUnit_addTest(pSuite, URITest, testSwap);
	CppUnit_addTest(pSuite, URITest, testEncodeDecode);
	CppUnit_addTest(pSuite, URITest, testOther);
	CppUnit_addTest(pSuite, URITest, testFromPath);
	CppUnit_addTest(pSuite, URITest, testQueryParameters);

	return pSuite;
}
