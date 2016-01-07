//
// AttributesImplTest.cpp
//
// $Id: //poco/1.4/XML/testsuite/src/AttributesImplTest.cpp#1 $
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

	assert (attrs.getLength() == 0);
	assert (attrs.getIndex("foo") == -1);
	assert (attrs.getValue("foo").empty());
	
	attrs.addAttribute("", "", "a1", "CDATA", "v1");
	assert (attrs.getLength() == 1);
	assert (attrs.getIndex("a1") == 0);
	assert (attrs.getQName(0) == "a1");
	assert (attrs.getType(0) == "CDATA");
	assert (attrs.getValue(0) == "v1");
	assert (attrs.isSpecified(0));
	
	assert (attrs.getType("a1") == "CDATA");
	assert (attrs.getValue("a1") == "v1");

	attrs.addAttribute("", "", "a2", "CDATA", "v2");
	assert (attrs.getLength() == 2);
	assert (attrs.getIndex("a2") == 1);
	assert (attrs.getQName(1) == "a2");
	assert (attrs.getType(1) == "CDATA");
	assert (attrs.getValue(1) == "v2");
	assert (attrs.isSpecified(1));
	
	assert (attrs.getType("a2") == "CDATA");
	assert (attrs.getValue("a2") == "v2");

	attrs.addAttribute("", "", "a3", "CDATA", "v3");
	assert (attrs.getLength() == 3);
	assert (attrs.getIndex("a3") == 2);	
	assert (attrs.getValue("a3") == "v3");
	
	attrs.removeAttribute(0);
	assert (attrs.getLength() == 2);
	assert (attrs.getIndex("a1") == -1);
	assert (attrs.getIndex("a2") == 0);
	assert (attrs.getIndex("a3") == 1);
	assert (attrs.getQName(0) == "a2");
	assert (attrs.getQName(1) == "a3");
	
	attrs.removeAttribute("a3");
	assert (attrs.getLength() == 1);
	assert (attrs.getIndex("a1") == -1);
	assert (attrs.getIndex("a2") == 0);
	assert (attrs.getIndex("a3") == -1);
	assert (attrs.getQName(0) == "a2");
}


void AttributesImplTest::testNamespaces()
{
	AttributesImpl attrs;

	assert (attrs.getLength() == 0);
	assert (attrs.getIndex("urn:ns", "foo") == -1);
	assert (attrs.getValue("urn:ns", "foo").empty());
	
	attrs.addAttribute("urn:ns", "a1", "p:a1", "CDATA", "v1");
	assert (attrs.getLength() == 1);
	assert (attrs.getIndex("urn:ns", "a1") == 0);
	assert (attrs.getQName(0) == "p:a1");
	assert (attrs.getLocalName(0) == "a1");
	assert (attrs.getURI(0) == "urn:ns");
	assert (attrs.getType(0) == "CDATA");
	assert (attrs.getValue(0) == "v1");
	assert (attrs.isSpecified(0));
	
	assert (attrs.getType("urn:ns", "a1") == "CDATA");
	assert (attrs.getValue("urn:ns", "a1") == "v1");

	attrs.addAttribute("urn:ns", "a2", "p:a2", "CDATA", "v2");
	assert (attrs.getLength() == 2);
	assert (attrs.getIndex("urn:ns", "a2") == 1);
	assert (attrs.getQName(1) == "p:a2");
	assert (attrs.getLocalName(1) == "a2");
	assert (attrs.getURI(1) == "urn:ns");
	assert (attrs.getType(1) == "CDATA");
	assert (attrs.getValue(1) == "v2");
	assert (attrs.isSpecified(1));
	
	assert (attrs.getType("urn:ns", "a2") == "CDATA");
	assert (attrs.getValue("urn:ns", "a2") == "v2");

	assert (attrs.getIndex("urn:ns2", "a2") == -1);

	attrs.addAttribute("urn:ns2", "a3", "q:a3", "CDATA", "v3");
	assert (attrs.getLength() == 3);
	assert (attrs.getIndex("urn:ns2", "a3") == 2);	
	assert (attrs.getValue("urn:ns2", "a3") == "v3");
	
	attrs.removeAttribute(0);
	assert (attrs.getLength() == 2);
	assert (attrs.getIndex("urn:ns", "a1") == -1);
	assert (attrs.getIndex("urn:ns", "a2") == 0);
	assert (attrs.getIndex("urn:ns2", "a3") == 1);
	assert (attrs.getQName(0) == "p:a2");
	assert (attrs.getLocalName(0) == "a2");
	assert (attrs.getURI(0) == "urn:ns");
	assert (attrs.getQName(1) == "q:a3");
	assert (attrs.getLocalName(1) == "a3");
	assert (attrs.getURI(1) == "urn:ns2");
	
	attrs.removeAttribute("urn:ns", "a3");
	assert (attrs.getLength() == 2);
	
	attrs.removeAttribute("urn:ns2", "a3");
	assert (attrs.getLength() == 1);
	assert (attrs.getIndex("urn:ns", "a1") == -1);
	assert (attrs.getIndex("urn:ns", "a2") == 0);
	assert (attrs.getIndex("urn:ns2", "a3") == -1);
	assert (attrs.getQName(0) == "p:a2");
}


void AttributesImplTest::testAccessors()
{
	AttributesImpl attrs;
	attrs.addAttribute("urn:ns1", "a1", "p:a1", "CDATA", "v1");
	attrs.addAttribute("urn:ns1", "a2", "p:a2", "CDATA", "v2", false);
	attrs.addAttribute("urn:ns2", "a3", "q:a3", "CDATA", "v3", true);

	assert (attrs.getQName(0) == "p:a1");
	assert (attrs.getQName(1) == "p:a2");
	assert (attrs.getQName(2) == "q:a3");

	assert (attrs.getLocalName(0) == "a1");
	assert (attrs.getLocalName(1) == "a2");
	assert (attrs.getLocalName(2) == "a3");

	assert (attrs.getURI(0) == "urn:ns1");
	assert (attrs.getURI(1) == "urn:ns1");
	assert (attrs.getURI(2) == "urn:ns2");

	assert (attrs.getValue(0) == "v1");
	assert (attrs.getValue(1) == "v2");
	assert (attrs.getValue(2) == "v3");

	assert (attrs.isSpecified(0));
	assert (!attrs.isSpecified(1));
	assert (attrs.isSpecified(2));

	attrs.setType(0, "NMTOKEN");
	assert (attrs.getType(0) == "NMTOKEN");
	assert (attrs.getType("urn:ns1", "a1") == "NMTOKEN");
	
	attrs.setValue(1, "v2 v2");
	assert (attrs.getValue(1) == "v2 v2");
	assert (attrs.getValue("urn:ns1", "a2") == "v2 v2");
	assert (attrs.isSpecified(1));
	
	attrs.setLocalName(2, "A3");
	assert (attrs.getLocalName(2) == "A3");
	attrs.setQName(2, "p:A3");
	assert (attrs.getQName(2) == "p:A3");
	attrs.setURI(2, "urn:ns1");
	assert (attrs.getURI(2) == "urn:ns1");
	
	assert (attrs.getValue("urn:ns1", "A3") == "v3");
}


void AttributesImplTest::testCopy()
{
	AttributesImpl attrs;
	attrs.addAttribute("urn:ns1", "a1", "p:a1", "CDATA", "v1");
	attrs.addAttribute("urn:ns1", "a2", "p:a2", "CDATA", "v2");
	attrs.addAttribute("urn:ns2", "a3", "q:a3", "CDATA", "v3");

	AttributesImpl attrs2;
	attrs2.setAttributes(attrs);

	assert (attrs2.getLength() == 3);
	
	assert (attrs2.getQName(0) == "p:a1");
	assert (attrs2.getQName(1) == "p:a2");
	assert (attrs2.getQName(2) == "q:a3");
	
	assert (attrs2.getLocalName(0) == "a1");
	assert (attrs2.getLocalName(1) == "a2");
	assert (attrs2.getLocalName(2) == "a3");

	assert (attrs2.getURI(0) == "urn:ns1");
	assert (attrs2.getURI(1) == "urn:ns1");
	assert (attrs2.getURI(2) == "urn:ns2");

	assert (attrs2.getValue(0) == "v1");
	assert (attrs2.getValue(1) == "v2");
	assert (attrs2.getValue(2) == "v3");
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
