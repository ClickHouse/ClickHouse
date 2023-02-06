//
// AttributesImplTest.cpp
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "AttributesImplTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/SAX/AttributesImpl.h"


using Poco::XML::AttributesImpl;


AttributesImplTest::AttributesImplTest(const std::string& name): CppUnit::TestCase(name)
{
}


AttributesImplTest::~AttributesImplTest()
{
}


void AttributesImplTest::testNoNamespaces()
{
	AttributesImpl attrs;

	assertTrue (attrs.getLength() == 0);
	assertTrue (attrs.getIndex("foo") == -1);
	assertTrue (attrs.getValue("foo").empty());
	
	attrs.addAttribute("", "", "a1", "CDATA", "v1");
	assertTrue (attrs.getLength() == 1);
	assertTrue (attrs.getIndex("a1") == 0);
	assertTrue (attrs.getQName(0) == "a1");
	assertTrue (attrs.getType(0) == "CDATA");
	assertTrue (attrs.getValue(0) == "v1");
	assertTrue (attrs.isSpecified(0));
	
	assertTrue (attrs.getType("a1") == "CDATA");
	assertTrue (attrs.getValue("a1") == "v1");

	attrs.addAttribute("", "", "a2", "CDATA", "v2");
	assertTrue (attrs.getLength() == 2);
	assertTrue (attrs.getIndex("a2") == 1);
	assertTrue (attrs.getQName(1) == "a2");
	assertTrue (attrs.getType(1) == "CDATA");
	assertTrue (attrs.getValue(1) == "v2");
	assertTrue (attrs.isSpecified(1));
	
	assertTrue (attrs.getType("a2") == "CDATA");
	assertTrue (attrs.getValue("a2") == "v2");

	attrs.addAttribute("", "", "a3", "CDATA", "v3");
	assertTrue (attrs.getLength() == 3);
	assertTrue (attrs.getIndex("a3") == 2);	
	assertTrue (attrs.getValue("a3") == "v3");
	
	attrs.removeAttribute(0);
	assertTrue (attrs.getLength() == 2);
	assertTrue (attrs.getIndex("a1") == -1);
	assertTrue (attrs.getIndex("a2") == 0);
	assertTrue (attrs.getIndex("a3") == 1);
	assertTrue (attrs.getQName(0) == "a2");
	assertTrue (attrs.getQName(1) == "a3");
	
	attrs.removeAttribute("a3");
	assertTrue (attrs.getLength() == 1);
	assertTrue (attrs.getIndex("a1") == -1);
	assertTrue (attrs.getIndex("a2") == 0);
	assertTrue (attrs.getIndex("a3") == -1);
	assertTrue (attrs.getQName(0) == "a2");
}


void AttributesImplTest::testNamespaces()
{
	AttributesImpl attrs;

	assertTrue (attrs.getLength() == 0);
	assertTrue (attrs.getIndex("urn:ns", "foo") == -1);
	assertTrue (attrs.getValue("urn:ns", "foo").empty());
	
	attrs.addAttribute("urn:ns", "a1", "p:a1", "CDATA", "v1");
	assertTrue (attrs.getLength() == 1);
	assertTrue (attrs.getIndex("urn:ns", "a1") == 0);
	assertTrue (attrs.getQName(0) == "p:a1");
	assertTrue (attrs.getLocalName(0) == "a1");
	assertTrue (attrs.getURI(0) == "urn:ns");
	assertTrue (attrs.getType(0) == "CDATA");
	assertTrue (attrs.getValue(0) == "v1");
	assertTrue (attrs.isSpecified(0));
	
	assertTrue (attrs.getType("urn:ns", "a1") == "CDATA");
	assertTrue (attrs.getValue("urn:ns", "a1") == "v1");

	attrs.addAttribute("urn:ns", "a2", "p:a2", "CDATA", "v2");
	assertTrue (attrs.getLength() == 2);
	assertTrue (attrs.getIndex("urn:ns", "a2") == 1);
	assertTrue (attrs.getQName(1) == "p:a2");
	assertTrue (attrs.getLocalName(1) == "a2");
	assertTrue (attrs.getURI(1) == "urn:ns");
	assertTrue (attrs.getType(1) == "CDATA");
	assertTrue (attrs.getValue(1) == "v2");
	assertTrue (attrs.isSpecified(1));
	
	assertTrue (attrs.getType("urn:ns", "a2") == "CDATA");
	assertTrue (attrs.getValue("urn:ns", "a2") == "v2");

	assertTrue (attrs.getIndex("urn:ns2", "a2") == -1);

	attrs.addAttribute("urn:ns2", "a3", "q:a3", "CDATA", "v3");
	assertTrue (attrs.getLength() == 3);
	assertTrue (attrs.getIndex("urn:ns2", "a3") == 2);	
	assertTrue (attrs.getValue("urn:ns2", "a3") == "v3");
	
	attrs.removeAttribute(0);
	assertTrue (attrs.getLength() == 2);
	assertTrue (attrs.getIndex("urn:ns", "a1") == -1);
	assertTrue (attrs.getIndex("urn:ns", "a2") == 0);
	assertTrue (attrs.getIndex("urn:ns2", "a3") == 1);
	assertTrue (attrs.getQName(0) == "p:a2");
	assertTrue (attrs.getLocalName(0) == "a2");
	assertTrue (attrs.getURI(0) == "urn:ns");
	assertTrue (attrs.getQName(1) == "q:a3");
	assertTrue (attrs.getLocalName(1) == "a3");
	assertTrue (attrs.getURI(1) == "urn:ns2");
	
	attrs.removeAttribute("urn:ns", "a3");
	assertTrue (attrs.getLength() == 2);
	
	attrs.removeAttribute("urn:ns2", "a3");
	assertTrue (attrs.getLength() == 1);
	assertTrue (attrs.getIndex("urn:ns", "a1") == -1);
	assertTrue (attrs.getIndex("urn:ns", "a2") == 0);
	assertTrue (attrs.getIndex("urn:ns2", "a3") == -1);
	assertTrue (attrs.getQName(0) == "p:a2");
}


void AttributesImplTest::testAccessors()
{
	AttributesImpl attrs;
	attrs.addAttribute("urn:ns1", "a1", "p:a1", "CDATA", "v1");
	attrs.addAttribute("urn:ns1", "a2", "p:a2", "CDATA", "v2", false);
	attrs.addAttribute("urn:ns2", "a3", "q:a3", "CDATA", "v3", true);

	assertTrue (attrs.getQName(0) == "p:a1");
	assertTrue (attrs.getQName(1) == "p:a2");
	assertTrue (attrs.getQName(2) == "q:a3");

	assertTrue (attrs.getLocalName(0) == "a1");
	assertTrue (attrs.getLocalName(1) == "a2");
	assertTrue (attrs.getLocalName(2) == "a3");

	assertTrue (attrs.getURI(0) == "urn:ns1");
	assertTrue (attrs.getURI(1) == "urn:ns1");
	assertTrue (attrs.getURI(2) == "urn:ns2");

	assertTrue (attrs.getValue(0) == "v1");
	assertTrue (attrs.getValue(1) == "v2");
	assertTrue (attrs.getValue(2) == "v3");

	assertTrue (attrs.isSpecified(0));
	assertTrue (!attrs.isSpecified(1));
	assertTrue (attrs.isSpecified(2));

	attrs.setType(0, "NMTOKEN");
	assertTrue (attrs.getType(0) == "NMTOKEN");
	assertTrue (attrs.getType("urn:ns1", "a1") == "NMTOKEN");
	
	attrs.setValue(1, "v2 v2");
	assertTrue (attrs.getValue(1) == "v2 v2");
	assertTrue (attrs.getValue("urn:ns1", "a2") == "v2 v2");
	assertTrue (attrs.isSpecified(1));
	
	attrs.setLocalName(2, "A3");
	assertTrue (attrs.getLocalName(2) == "A3");
	attrs.setQName(2, "p:A3");
	assertTrue (attrs.getQName(2) == "p:A3");
	attrs.setURI(2, "urn:ns1");
	assertTrue (attrs.getURI(2) == "urn:ns1");
	
	assertTrue (attrs.getValue("urn:ns1", "A3") == "v3");
}


void AttributesImplTest::testCopy()
{
	AttributesImpl attrs;
	attrs.addAttribute("urn:ns1", "a1", "p:a1", "CDATA", "v1");
	attrs.addAttribute("urn:ns1", "a2", "p:a2", "CDATA", "v2");
	attrs.addAttribute("urn:ns2", "a3", "q:a3", "CDATA", "v3");

	AttributesImpl attrs2;
	attrs2.setAttributes(attrs);

	assertTrue (attrs2.getLength() == 3);
	
	assertTrue (attrs2.getQName(0) == "p:a1");
	assertTrue (attrs2.getQName(1) == "p:a2");
	assertTrue (attrs2.getQName(2) == "q:a3");
	
	assertTrue (attrs2.getLocalName(0) == "a1");
	assertTrue (attrs2.getLocalName(1) == "a2");
	assertTrue (attrs2.getLocalName(2) == "a3");

	assertTrue (attrs2.getURI(0) == "urn:ns1");
	assertTrue (attrs2.getURI(1) == "urn:ns1");
	assertTrue (attrs2.getURI(2) == "urn:ns2");

	assertTrue (attrs2.getValue(0) == "v1");
	assertTrue (attrs2.getValue(1) == "v2");
	assertTrue (attrs2.getValue(2) == "v3");
}


void AttributesImplTest::setUp()
{
}


void AttributesImplTest::tearDown()
{
}


CppUnit::Test* AttributesImplTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("AttributesImplTest");

	CppUnit_addTest(pSuite, AttributesImplTest, testNoNamespaces);
	CppUnit_addTest(pSuite, AttributesImplTest, testNamespaces);
	CppUnit_addTest(pSuite, AttributesImplTest, testAccessors);
	CppUnit_addTest(pSuite, AttributesImplTest, testCopy);

	return pSuite;
}
