//
// NameTest.cpp
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "NameTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/XML/Name.h"


using Poco::XML::Name;


NameTest::NameTest(const std::string& name): CppUnit::TestCase(name)
{
}


NameTest::~NameTest()
{
}


void NameTest::testSplit()
{
	std::string qname = "name";
	std::string prefix;
	std::string local;
	Name::split(qname, prefix, local);
	assertTrue (prefix.empty());
	assertTrue (local == "name");
	
	qname = "p:l";
	Name::split(qname, prefix, local);
	assertTrue (prefix == "p");
	assertTrue (local == "l");
	
	qname = "pre:local";
	Name::split(qname, prefix, local);
	assertTrue (prefix == "pre");
	assertTrue (local == "local");
}


void NameTest::testLocalName()
{
	std::string qname = "name";
	std::string local = Name::localName(qname);
	assertTrue (local == "name");
	qname = "p:l";
	local = Name::localName(qname);
	assertTrue (local == "l");
	qname = "pre:local";
	local = Name::localName(qname);
	assertTrue (local == "local");
}


void NameTest::testPrefix()
{
	std::string qname = "name";
	std::string prefix = Name::prefix(qname);
	assertTrue (prefix.empty());
	qname = "p:l";
	prefix = Name::prefix(qname);
	assertTrue (prefix == "p");
	qname = "pre:local";
	prefix = Name::prefix(qname);
	assertTrue (prefix == "pre");
}


void NameTest::testName()
{
	std::string qname = "name";
	Name name(qname);
	assertTrue (name.qname() == "name");
	assertTrue (name.prefix().empty());
	assertTrue (name.namespaceURI().empty());
	assertTrue (name.localName().empty());

	qname.clear();
	name.assign(qname, "http://www.appinf.com/", "local");
	assertTrue (name.qname().empty());
	assertTrue (name.prefix().empty());
	assertTrue (name.namespaceURI() == "http://www.appinf.com/");
	assertTrue (name.localName() == "local");

	Name name2("pre:local", "http://www.appinf.com/");
	assertTrue (name2.qname() == "pre:local");
	assertTrue (name2.prefix() == "pre");
	assertTrue (name2.namespaceURI() == "http://www.appinf.com/");
	assertTrue (name2.localName() == "local");

	name2.assign("PRE:Local", "http://www.appinf.com/");
	assertTrue (name2.qname() == "PRE:Local");
	assertTrue (name2.prefix() == "PRE");
	assertTrue (name2.namespaceURI() == "http://www.appinf.com/");
	assertTrue (name2.localName() == "Local");
}


void NameTest::testCompare()
{
	Name n1("pre:local");
	Name n2(n1);
	Name n3("pre:local2");
	
	assertTrue (n1.equals(n2));
	assertTrue (!n1.equals(n3));
	
	n1.assign("pre:local", "http://www.appinf.com", "local");
	n2.assign("pre:local", "http://www.appinf.com", "local");
	n3.assign("pre:local2", "http://www.appinf.com", "local2");
	
	assertTrue (n1.equals(n2));
	assertTrue (!n1.equals(n3));
	
	assertTrue (n1.equals("pre:local", "http://www.appinf.com", "local"));
	assertTrue (!n1.equals("pre:local", "", ""));
	assertTrue (n1.equalsWeakly("pre:local", "", ""));
	assertTrue (!n1.equalsWeakly("pre:local2", "", ""));
	assertTrue (!n1.equals("", "http://www.appinf.com", "local"));
	assertTrue (n1.equalsWeakly("", "http://www.appinf.com", "local"));
	assertTrue (!n1.equalsWeakly("", "http://www.appinf.com", "local2"));
}


void NameTest::testSwap()
{
	Name n1("ns:name1", "http://www.appinf.com");
	Name n2("ns:name2", "http://www.foobar.com");
	n1.swap(n2);
	assertTrue (n1.qname() == "ns:name2");
	assertTrue (n1.namespaceURI() == "http://www.foobar.com");
	assertTrue (n1.localName() == "name2");
	assertTrue (n2.qname() == "ns:name1");
	assertTrue (n2.namespaceURI() == "http://www.appinf.com");
	assertTrue (n2.localName() == "name1");
}


void NameTest::setUp()
{
}


void NameTest::tearDown()
{
}


CppUnit::Test* NameTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("NameTest");

	CppUnit_addTest(pSuite, NameTest, testSplit);
	CppUnit_addTest(pSuite, NameTest, testLocalName);
	CppUnit_addTest(pSuite, NameTest, testPrefix);
	CppUnit_addTest(pSuite, NameTest, testName);
	CppUnit_addTest(pSuite, NameTest, testCompare);
	CppUnit_addTest(pSuite, NameTest, testSwap);

	return pSuite;
}
