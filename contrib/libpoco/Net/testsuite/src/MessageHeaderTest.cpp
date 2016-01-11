//
// MessageHeaderTest.cpp
//
// $Id: //poco/1.4/Net/testsuite/src/MessageHeaderTest.cpp#3 $
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "MessageHeaderTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Net/MessageHeader.h"
#include "Poco/Net/NetException.h"
#include <sstream>


using Poco::Net::MessageHeader;
using Poco::Net::NameValueCollection;
using Poco::Net::MessageException;


MessageHeaderTest::MessageHeaderTest(const std::string& name): CppUnit::TestCase(name)
{
}


MessageHeaderTest::~MessageHeaderTest()
{
}


void MessageHeaderTest::testWrite()
{
	MessageHeader mh;
	mh.set("name1", "value1");
	mh.set("name2", "value2");
	mh.set("name3", "value3");
	
	std::ostringstream ostr;
	mh.write(ostr);
	std::string s = ostr.str();
	assert (s == "name1: value1\r\nname2: value2\r\nname3: value3\r\n");
}


void MessageHeaderTest::testRead1()
{
	std::string s("name1: value1\r\nname2: value2\r\nname3: value3\r\n");
	std::istringstream istr(s);
	MessageHeader mh;
	mh.read(istr);
	assert (mh.size() == 3);
	assert (mh["name1"] == "value1");
	assert (mh["name2"] == "value2");
	assert (mh["name3"] == "value3");
}


void MessageHeaderTest::testRead2()
{
	std::string s("name1: value1\nname2: value2\nname3: value3\n");
	std::istringstream istr(s);
	MessageHeader mh;
	mh.read(istr);
	assert (mh.size() == 3);
	assert (mh["name1"] == "value1");
	assert (mh["name2"] == "value2");
	assert (mh["name3"] == "value3");
}


void MessageHeaderTest::testRead3()
{
	std::string s("name1: value1\r\n");
	std::istringstream istr(s);
	MessageHeader mh;
	mh.read(istr);
	assert (mh.size() == 1);
	assert (mh["name1"] == "value1");
}



void MessageHeaderTest::testRead4()
{
	std::string s("name1: value1\r\nname2: value2\r\n\r\nsomedata");
	std::istringstream istr(s);
	MessageHeader mh;
	mh.read(istr);
	assert (mh.size() == 2);
	assert (mh["name1"] == "value1");
	assert (mh["name2"] == "value2");
	int ch = istr.get();
	assert (ch == '\r');
	ch = istr.get();
	assert (ch == '\n');
	ch = istr.get();
	assert (ch == 's');
}


void MessageHeaderTest::testRead5()
{
	std::string s("name1:\r\nname2: value2\r\nname3: value3  \r\n");
	std::istringstream istr(s);
	MessageHeader mh;
	mh.read(istr);
	assert (mh.size() == 3);
	assert (mh["name1"] == "");
	assert (mh["name2"] == "value2");
	assert (mh["name3"] == "value3");
}


void MessageHeaderTest::testReadFolding1()
{
	std::string s("name1: value1\r\nname2: value21\r\n value22\r\nname3: value3\r\n");
	std::istringstream istr(s);
	MessageHeader mh;
	mh.read(istr);
	assert (mh.size() == 3);
	assert (mh["name1"] == "value1");
	assert (mh["name2"] == "value21 value22");
	assert (mh["name3"] == "value3");
}


void MessageHeaderTest::testReadFolding2()
{
	std::string s("name1: value1\nname2: value21\n\tvalue22\nname3: value3\n");
	std::istringstream istr(s);
	MessageHeader mh;
	mh.read(istr);
	assert (mh.size() == 3);
	assert (mh["name1"] == "value1");
	assert (mh["name2"] == "value21\tvalue22");
	assert (mh["name3"] == "value3");
}


void MessageHeaderTest::testReadFolding3()
{
	std::string s("name1: value1\r\nname2: value21\r\n value22\r\n");
	std::istringstream istr(s);
	MessageHeader mh;
	mh.read(istr);
	assert (mh.size() == 2);
	assert (mh["name1"] == "value1");
	assert (mh["name2"] == "value21 value22");
}


void MessageHeaderTest::testReadFolding4()
{
	std::string s("name1: value1\r\nname2: value21\r\n value22\r\n value23");
	std::istringstream istr(s);
	MessageHeader mh;
	mh.read(istr);
	assert (mh.size() == 2);
	assert (mh["name1"] == "value1");
	assert (mh["name2"] == "value21 value22 value23");
}


void MessageHeaderTest::testReadFolding5()
{
	std::string s("name1: value1\r\nname2: value21\r\n value22\r\n value23\r\nname3: value3");
	std::istringstream istr(s);
	MessageHeader mh;
	mh.read(istr);
	assert (mh.size() == 3);
	assert (mh["name1"] == "value1");
	assert (mh["name2"] == "value21 value22 value23");
	assert (mh["name3"] == "value3");
}


void MessageHeaderTest::testReadInvalid1()
{
	std::string s("name1: value1\r\nname2: value21\r\n value22\r\n value23\r\n");
	s.append(300, 'x');
	std::istringstream istr(s);
	MessageHeader mh;
	try
	{
		mh.read(istr);
		fail("malformed message - must throw");
	}
	catch (MessageException&)
	{
	}
}


void MessageHeaderTest::testReadInvalid2()
{
	std::string s("name1: value1\r\nname2: ");
	s.append(9000, 'x');
	std::istringstream istr(s);
	MessageHeader mh;
	try
	{
		mh.read(istr);
		fail("malformed message - must throw");
	}
	catch (MessageException&)
	{
	}
}


void MessageHeaderTest::testSplitElements()
{
	std::string s;
	std::vector<std::string> v;
	MessageHeader::splitElements(s, v);
	assert (v.empty());
	
	s = "foo";
	MessageHeader::splitElements(s, v);
	assert (v.size() == 1);
	assert (v[0] == "foo");
	
	s = "  foo ";
	MessageHeader::splitElements(s, v);
	assert (v.size() == 1);
	assert (v[0] == "foo");

	s = "foo,bar";
	MessageHeader::splitElements(s, v);
	assert (v.size() == 2);
	assert (v[0] == "foo");
	assert (v[1] == "bar");
	
	s = "foo,,bar";
	MessageHeader::splitElements(s, v);
	assert (v.size() == 2);
	assert (v[0] == "foo");
	assert (v[1] == "bar");

	MessageHeader::splitElements(s, v, false);
	assert (v.size() == 3);
	assert (v[0] == "foo");
	assert (v[1] == "");
	assert (v[2] == "bar");

	s = "foo;param=\"a,b\",bar;param=\"c,d\"";
	MessageHeader::splitElements(s, v);
	assert (v.size() == 2);
	assert (v[0] == "foo;param=\"a,b\"");
	assert (v[1] == "bar;param=\"c,d\"");

	s = "foo; param=\"a,b\",  bar; param=\"c,d\"";
	MessageHeader::splitElements(s, v);
	assert (v.size() == 2);
	assert (v[0] == "foo; param=\"a,b\"");
	assert (v[1] == "bar; param=\"c,d\"");
	
	s = "foo, bar, f00, baz";
	MessageHeader::splitElements(s, v);
	assert (v.size() == 4);
	assert (v[0] == "foo");
	assert (v[1] == "bar");
	assert (v[2] == "f00");
	assert (v[3] == "baz");
	
	s = "a,b,c";
	MessageHeader::splitElements(s, v);
	assert (v.size() == 3);
	assert (v[0] == "a");
	assert (v[1] == "b");
	assert (v[2] == "c");
	
	s = "a=\"value=\\\\\\\"foo, bar\\\\\\\"\",b=foo";
	MessageHeader::splitElements(s, v);
	assert (v.size() == 2);
	assert (v[0] == "a=\"value=\\\"foo, bar\\\"\"");
	assert (v[1] == "b=foo");
	
	s = "a=\\\",b=\\\"";
	MessageHeader::splitElements(s, v);
	assert (v.size() == 2);
	assert (v[0] == "a=\"");
	assert (v[1] == "b=\"");
	
}


void MessageHeaderTest::testSplitParameters()
{
	std::string s;
	std::string v;
	NameValueCollection p;
	
	MessageHeader::splitParameters(s, v, p);
	assert (v.empty());
	assert (p.empty());
	
	s = "multipart/related";
	MessageHeader::splitParameters(s, v, p);
	assert (v == "multipart/related");
	assert (p.empty());
	
	s = "multipart/related; boundary=MIME_boundary_01234567";
	MessageHeader::splitParameters(s, v, p);
	assert (v == "multipart/related");
	assert (p.size() == 1);
	assert (p["boundary"] == "MIME_boundary_01234567");
	
	s = "multipart/related; boundary=\"MIME_boundary_76543210\"";
	MessageHeader::splitParameters(s, v, p);
	assert (v == "multipart/related");
	assert (p.size() == 1);
	assert (p["boundary"] == "MIME_boundary_76543210");
	
	s = "text/plain; charset=us-ascii";
	MessageHeader::splitParameters(s, v, p);
	assert (v == "text/plain");
	assert (p.size() == 1);
	assert (p["charset"] == "us-ascii");
	
	s = "value; p1=foo; p2=bar";
	MessageHeader::splitParameters(s, v, p);
	assert (v == "value");
	assert (p.size() == 2);
	assert (p["p1"] == "foo");
	assert (p["p2"] == "bar");
	
	s = "value; p1=\"foo; bar\"";
	MessageHeader::splitParameters(s, v, p);
	assert (v == "value");
	assert (p.size() == 1);
	assert (p["p1"] == "foo; bar");	

	s = "value ; p1=foo ; p2=bar ";
	MessageHeader::splitParameters(s, v, p);
	assert (v == "value");
	assert (p.size() == 2);
	assert (p["p1"] == "foo");
	assert (p["p2"] == "bar");
}


void MessageHeaderTest::testFieldLimit()
{
	std::string s("name1: value1\r\nname2: value2\r\nname3: value3\r\n");
	std::istringstream istr(s);
	MessageHeader mh;
	mh.setFieldLimit(2);
	try
	{
		mh.read(istr);
		fail("Field limit exceeded - must throw");
	}
	catch (MessageException&)
	{
	}
}


void MessageHeaderTest::setUp()
{
}


void MessageHeaderTest::tearDown()
{
}


CppUnit::Test* MessageHeaderTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("MessageHeaderTest");

	CppUnit_addTest(pSuite, MessageHeaderTest, testWrite);
	CppUnit_addTest(pSuite, MessageHeaderTest, testRead1);
	CppUnit_addTest(pSuite, MessageHeaderTest, testRead2);
	CppUnit_addTest(pSuite, MessageHeaderTest, testRead3);
	CppUnit_addTest(pSuite, MessageHeaderTest, testRead4);
	CppUnit_addTest(pSuite, MessageHeaderTest, testRead5);
	CppUnit_addTest(pSuite, MessageHeaderTest, testReadFolding1);
	CppUnit_addTest(pSuite, MessageHeaderTest, testReadFolding2);
	CppUnit_addTest(pSuite, MessageHeaderTest, testReadFolding3);
	CppUnit_addTest(pSuite, MessageHeaderTest, testReadFolding4);
	CppUnit_addTest(pSuite, MessageHeaderTest, testReadFolding5);
	CppUnit_addTest(pSuite, MessageHeaderTest, testReadInvalid1);
	CppUnit_addTest(pSuite, MessageHeaderTest, testReadInvalid2);
	CppUnit_addTest(pSuite, MessageHeaderTest, testSplitElements);
	CppUnit_addTest(pSuite, MessageHeaderTest, testSplitParameters);
	CppUnit_addTest(pSuite, MessageHeaderTest, testFieldLimit);

	return pSuite;
}
