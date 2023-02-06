//
// NamespaceSupportTest.cpp
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
	assertTrue (prefixes.size() == 2);
	assertTrue (prefixes.find("xml") != prefixes.end());
	assertTrue (prefixes.find("xmlns") != prefixes.end());
	
	ns.getPrefixes(prefixes);
	assertTrue (prefixes.size() == 2);
	assertTrue (prefixes.find("xml") != prefixes.end());
	assertTrue (prefixes.find("xmlns") != prefixes.end());
	
	ns.pushContext();
	ns.declarePrefix("ns1", "urn:ns1");
	ns.declarePrefix("ns2", "urn:ns2");
	
	ns.getDeclaredPrefixes(prefixes);
	assertTrue (prefixes.size() == 2);
	assertTrue (prefixes.find("ns1") != prefixes.end());
	assertTrue (prefixes.find("ns2") != prefixes.end());

	ns.pushContext();
	ns.declarePrefix("ns3", "urn:ns3");
	
	ns.getDeclaredPrefixes(prefixes);
	assertTrue (prefixes.size() == 1);
	assertTrue (prefixes.find("ns3") != prefixes.end());

	ns.getPrefixes(prefixes);
	assertTrue (prefixes.size() == 5);
	assertTrue (prefixes.find("xml") != prefixes.end());
	assertTrue (prefixes.find("xmlns") != prefixes.end());
	assertTrue (prefixes.find("ns1") != prefixes.end());
	assertTrue (prefixes.find("ns2") != prefixes.end());
	assertTrue (prefixes.find("ns3") != prefixes.end());

	ns.popContext();
	ns.getDeclaredPrefixes(prefixes);
	assertTrue (prefixes.size() == 2);
	assertTrue (prefixes.find("ns1") != prefixes.end());
	assertTrue (prefixes.find("ns2") != prefixes.end());

	assertTrue (ns.isMapped("urn:ns1"));
	assertTrue (ns.isMapped("urn:ns2"));
	assertTrue (ns.isMapped("http://www.w3.org/XML/1998/namespace"));
	assertTrue (!ns.isMapped("urn:ns3"));
	
	ns.getPrefixes("urn:ns2", prefixes);
	assertTrue (prefixes.size() == 1);
	assertTrue (prefixes.find("ns2") != prefixes.end());
	
	ns.pushContext();
	ns.declarePrefix("", "urn:ns3");
	ns.declarePrefix("NS2", "urn:ns2");
	
	ns.getPrefixes("urn:ns2", prefixes);
	assertTrue (prefixes.size() == 2);
	assertTrue (prefixes.find("ns2") != prefixes.end());
	assertTrue (prefixes.find("NS2") != prefixes.end());
	
	ns.getPrefixes(prefixes);
	assertTrue (prefixes.size() == 5);
	assertTrue (prefixes.find("xml") != prefixes.end());
	assertTrue (prefixes.find("xmlns") != prefixes.end());
	assertTrue (prefixes.find("ns1") != prefixes.end());
	assertTrue (prefixes.find("ns2") != prefixes.end());
	assertTrue (prefixes.find("NS2") != prefixes.end());

	ns.getDeclaredPrefixes(prefixes);
	assertTrue (prefixes.size() == 2);
	assertTrue (prefixes.find("") != prefixes.end());
	assertTrue (prefixes.find("NS2") != prefixes.end());
	
	assertTrue (ns.getPrefix("urn:ns3") == "");
	assertTrue (ns.getPrefix("urn:ns2") == "NS2");
	assertTrue (ns.getPrefix("urn:ns4") == "");
	
	assertTrue (ns.isMapped("urn:ns3"));
	assertTrue (ns.isMapped("urn:ns2"));
	assertTrue (!ns.isMapped("urn:ns4"));

	assertTrue (ns.getURI("xml") == "http://www.w3.org/XML/1998/namespace");
	assertTrue (ns.getURI("ns1") == "urn:ns1");
	assertTrue (ns.getURI("") == "urn:ns3");
	assertTrue (ns.getURI("NS2") == "urn:ns2");
	
	std::string localName;
	std::string namespaceURI;
	bool declared = ns.processName("elem", namespaceURI, localName, false);
	assertTrue (declared);
	assertTrue (localName == "elem");
	assertTrue (namespaceURI == "urn:ns3");
	
	declared = ns.processName("NS2:elem", namespaceURI, localName, false);
	assertTrue (declared);
	assertTrue (localName == "elem");
	assertTrue (namespaceURI == "urn:ns2");

	declared = ns.processName("ns3:elem", namespaceURI, localName, false);
	assertTrue (!declared);
	assertTrue (localName == "elem");
	assertTrue (namespaceURI == "");

	declared = ns.processName("ns2:attr", namespaceURI, localName, true);
	assertTrue (declared);
	assertTrue (localName == "attr");
	assertTrue (namespaceURI == "urn:ns2");

	declared = ns.processName("attr", namespaceURI, localName, true);
	assertTrue (declared);
	assertTrue (localName == "attr");
	assertTrue (namespaceURI == "");

	declared = ns.processName("ns3:attr", namespaceURI, localName, true);
	assertTrue (!declared);
	assertTrue (localName == "attr");
	assertTrue (namespaceURI == "");
	
	ns.popContext();
	assertTrue (ns.getURI("xml") == "http://www.w3.org/XML/1998/namespace");
	assertTrue (ns.getURI("ns1") == "urn:ns1");
	assertTrue (ns.getURI("") == "");
	assertTrue (ns.getURI("NS2") == "");
	
	declared = ns.processName("elem", namespaceURI, localName, false);
	assertTrue (declared);
	assertTrue (localName == "elem");
	assertTrue (namespaceURI == "");
	
	declared = ns.processName("ns2:elem", namespaceURI, localName, false);
	assertTrue (declared);
	assertTrue (localName == "elem");
	assertTrue (namespaceURI == "urn:ns2");

	declared = ns.processName("ns3:elem", namespaceURI, localName, false);
	assertTrue (!declared);
	assertTrue (localName == "elem");
	assertTrue (namespaceURI == "");
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
