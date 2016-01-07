//
// DNSTest.cpp
//
// $Id: //poco/1.4/Net/testsuite/src/DNSTest.cpp#4 $
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DNSTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Net/DNS.h"
#include "Poco/Net/HostEntry.h"
#include "Poco/Net/NetException.h"


using Poco::Net::DNS;
using Poco::Net::IPAddress;
using Poco::Net::HostEntry;
using Poco::Net::InvalidAddressException;
using Poco::Net::HostNotFoundException;
using Poco::Net::ServiceNotFoundException;
using Poco::Net::NoAddressFoundException;


DNSTest::DNSTest(const std::string& name): CppUnit::TestCase(name)
{
}


DNSTest::~DNSTest()
{
}


void DNSTest::testHostByName()
{
	HostEntry he1 = DNS::hostByName("aliastest.appinf.com");
	// different systems report different canonical names, unfortunately.
	assert (he1.name() == "dnstest.appinf.com" || he1.name() == "aliastest.appinf.com");
#if !defined(POCO_HAVE_ADDRINFO)
	// getaddrinfo() does not report any aliases
	assert (!he1.aliases().empty());
	assert (he1.aliases()[0] == "aliastest.appinf.com");
#endif
	assert (he1.addresses().size() >= 1);
	assert (he1.addresses()[0].toString() == "1.2.3.4");
	
	try
	{
		HostEntry he1 = DNS::hostByName("nohost.appinf.com");
		fail("host not found - must throw");
	}
	catch (HostNotFoundException&)
	{
	}
	catch (NoAddressFoundException&)
	{
	}
}


void DNSTest::testHostByAddress()
{
	IPAddress ip1("80.122.195.86");
	HostEntry he1 = DNS::hostByAddress(ip1);
	assert (he1.name() == "mailhost.appinf.com");
	assert (he1.aliases().empty());
	assert (he1.addresses().size() >= 1);
	assert (he1.addresses()[0].toString() == "80.122.195.86");
	
	IPAddress ip2("10.0.244.253");
	try
	{
		HostEntry he2 = DNS::hostByAddress(ip2);
		fail("host not found - must throw");
	}
	catch (HostNotFoundException&)
	{
	}
	catch (NoAddressFoundException&)
	{
	}
}


void DNSTest::testResolve()
{
}


void DNSTest::setUp()
{
}


void DNSTest::tearDown()
{
}


CppUnit::Test* DNSTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("DNSTest");

	CppUnit_addTest(pSuite, DNSTest, testHostByName);
	CppUnit_addTest(pSuite, DNSTest, testHostByAddress);
	CppUnit_addTest(pSuite, DNSTest, testResolve);

	return pSuite;
}
