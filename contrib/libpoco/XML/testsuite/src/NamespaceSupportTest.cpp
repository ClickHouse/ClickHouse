//
// NamespaceSupportTest.cpp
//
// $Id: //poco/1.4/XML/testsuite/src/NamespaceSupportTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "NamespaceSupportTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/SAX/NamespaceSupport.h"


using Poco::XML::NamespaceSupport;


NamespaceSupportTest::NamespaceSupportTest(const std::string& name): CppUnit::TestCase(name)
{
}


NamespaceSupportTest::~NamespaceSupportTest()
{
}


void NamespaceSupportTest::testNamespaceSupport()
{
	NamespaceSupport ns;
	NamespaceSupport::PrefixSet prefixes;
	ns.getDeclaredPrefixes(prefixes);
	assert (prefixes.size() == 2);
	assert (prefixes.find("xml") != prefixes.end());
	assert (prefixes.find("xmlns") != prefixes.end());
	
	ns.getPrefixes(prefixes);
	assert (prefixes.size() == 2);
	assert (prefixes.find("xml") != prefixes.end());
	assert (prefixes.find("xmlns") != prefixes.end());
	
	ns.pushContext();
	ns.declarePrefix("ns1", "urn:ns1");
	ns.declarePrefix("ns2", "urn:ns2");
	
	ns.getDeclaredPrefixes(prefixes);
	assert (prefixes.size() == 2);
	assert (prefixes.find("ns1") != prefixes.end());
	assert (prefixes.find("ns2") != prefixes.end());

	ns.pushContext();
	ns.declarePrefix("ns3", "urn:ns3");
	
	ns.getDeclaredPrefixes(prefixes);
	assert (prefixes.size() == 1);
	assert (prefixes.find("ns3") != prefixes.end());

	ns.getPrefixes(prefixes);
	assert (prefixes.size() == 5);
	assert (prefixes.find("xml") != prefixes.end());
	assert (prefixes.find("xmlns") != prefixes.end());
	assert (prefixes.find("ns1") != prefixes.end());
	assert (prefixes.find("ns2") != prefixes.end());
	assert (prefixes.find("ns3") != prefixes.end());

	ns.popContext();
	ns.getDeclaredPrefixes(prefixes);
	assert (prefixes.size() == 2);
	assert (prefixes.find("ns1") != prefixes.end());
	assert (prefixes.find("ns2") != prefixes.end());

	assert (ns.isMapped("urn:ns1"));
	assert (ns.isMapped("urn:ns2"));
	assert (ns.isMapped("http://www.w3.org/XML/1998/namespace"));
	assert (!ns.isMapped("urn:ns3"));
	
	ns.getPrefixes("urn:ns2", prefixes);
	assert (prefixes.size() == 1);
	assert (prefixes.find("ns2") != prefixes.end());
	
	ns.pushContext();
	ns.declarePrefix("", "urn:ns3");
	ns.declarePrefix("NS2", "urn:ns2");
	
	ns.getPrefixes("urn:ns2", prefixes);
	assert (prefixes.size() == 2);
	assert (prefixes.find("ns2") != prefixes.end());
	assert (prefixes.find("NS2") != prefixes.end());
	
	ns.getPrefixes(prefixes);
	assert (prefixes.size() == 5);
	assert (prefixes.find("xml") != prefixes.end());
	assert (prefixes.find("xmlns") != prefixes.end());
	assert (prefixes.find("ns1") != prefixes.end());
	assert (prefixes.find("ns2") != prefixes.end());
	assert (prefixes.find("NS2") != prefixes.end());

	ns.getDeclaredPrefixes(prefixes);
	assert (prefixes.size() == 2);
	assert (prefixes.find("") != prefixes.end());
	assert (prefixes.find("NS2") != prefixes.end());
	
	assert (ns.getPrefix("urn:ns3") == "");
	assert (ns.getPrefix("urn:ns2") == "NS2");
	assert (ns.getPrefix("urn:ns4") == "");
	
	assert (ns.isMapped("urn:ns3"));
	assert (ns.isMapped("urn:ns2"));
	assert (!ns.isMapped("urn:ns4"));

	assert (ns.getURI("xml") == "http://www.w3.org/XML/1998/namespace");
	assert (ns.getURI("ns1") == "urn:ns1");
	assert (ns.getURI("") == "urn:ns3");
	assert (ns.getURI("NS2") == "urn:ns2");
	
	std::string localName;
	std::string namespaceURI;
	bool declared = ns.processName("elem", namespaceURI, localName, false);
	assert (declared);
	assert (localName == "elem");
	assert (namespaceURI == "urn:ns3");
	
	declared = ns.processName("NS2:elem", namespaceURI, localName, false);
	assert (declared);
	assert (localName == "elem");
	assert (namespaceURI == "urn:ns2");

	declared = ns.processName("ns3:elem", namespaceURI, localName, false);
	assert (!declared);
	assert (localName == "elem");
	assert (namespaceURI == "");

	declared = ns.processName("ns2:attr", namespaceURI, localName, true);
	assert (declared);
	assert (localName == "attr");
	assert (namespaceURI == "urn:ns2");

	declared = ns.processName("attr", namespaceURI, localName, true);
	assert (declared);
	assert (localName == "attr");
	assert (namespaceURI == "");

	declared = ns.processName("ns3:attr", namespaceURI, localName, true);
	assert (!declared);
	assert (localName == "attr");
	assert (namespaceURI == "");
	
	ns.popContext();
	assert (ns.getURI("xml") == "http://www.w3.org/XML/1998/namespace");
	assert (ns.getURI("ns1") == "urn:ns1");
	assert (ns.getURI("") == "");
	assert (ns.getURI("NS2") == "");
	
	declared = ns.processName("elem", namespaceURI, localName, false);
	assert (declared);
	assert (localName == "elem");
	assert (namespaceURI == "");
	
	declared = ns.processName("ns2:elem", namespaceURI, localName, false);
	assert (declared);
	assert (localName == "elem");
	assert (namespaceURI == "urn:ns2");

	declared = ns.processName("ns3:elem", namespaceURI, localName, false);
	assert (!declared);
	assert (localName == "elem");
	assert (namespaceURI == "");
}


void NamespaceSupportTest::setUp()
{
}


void NamespaceSupportTest::tearDown()
{
}


CppUnit::Test* NamespaceSupportTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("NamespaceSupportTest");

	CppUnit_addTest(pSuite, NamespaceSupportTest, testNamespaceSupport);

	return pSuite;
}
