//
// DNSTest.cpp
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
	HostEntry he1 = DNS::hostByName("aliastest.pocoproject.org");
	// different systems report different canonical names, unfortunately.
	assert (he1.name() == "dnstest.pocoproject.org" || he1.name() == "aliastest.pocoproject.org");
#if !defined(POCO_HAVE_ADDRINFO)
	// getaddrinfo() does not report any aliases
	assert (!he1.aliases().empty());
	assert (he1.aliases()[0] == "aliastest.pocoproject.org");
#endif
	assert (he1.addresses().size() >= 1);
	assert (he1.addresses()[0].toString() == "1.2.3.4");

	try
	{
		HostEntry he1 = DNS::hostByName("nohost.pocoproject.org");
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


void DNSTest::testEncodeIDN()
{
	std::string idn("d\xc3\xb6m\xc3\xa4in.example"); // d"om"ain.example 
	assert (DNS::isIDN(idn));
	assert (DNS::encodeIDN(idn) == "xn--dmin-moa0i.example");

	idn = ".d\xc3\xb6m\xc3\xa4in.example"; // .d"om"ain.example 
	assert(DNS::isIDN(idn));
	assert(DNS::encodeIDN(idn) == ".xn--dmin-moa0i.example");

	idn = "d\xc3\xb6m\xc3\xa4in.example."; // .d"om"ain.example.
	assert(DNS::isIDN(idn));
	assert(DNS::encodeIDN(idn) == "xn--dmin-moa0i.example.");

	idn = "d\xc3\xb6m\xc3\xa4in"; // d"om"ain
	assert(DNS::isIDN(idn));
	assert(DNS::encodeIDN(idn) == "xn--dmin-moa0i");

	idn = "\xc3\xa4""aaa.example"; // "aaaa.example
	assert (DNS::isIDN(idn));
	assert (DNS::encodeIDN(idn) == "xn--aaa-pla.example");

	idn = "a\xc3\xa4""aa.example"; // a"aaa.example
	assert(DNS::isIDN(idn));
	assert(DNS::encodeIDN(idn) == "xn--aaa-qla.example");

	idn = "foo.\xc3\xa2""bcd\xc3\xa9""f.example"; // foo.^abcd'ef.example
	assert(DNS::isIDN(idn));
	assert(DNS::encodeIDN(idn) == "foo.xn--bcdf-9na9b.example");

	idn = "\xe2\x98\x83.example"; // <snowman>.example
	assert(DNS::isIDN(idn));
	assert(DNS::encodeIDN(idn) == "xn--n3h.example");

	idn = "\xe2\x98\x83."; // <snowman>.
	assert(DNS::isIDN(idn));
	assert(DNS::encodeIDN(idn) == "xn--n3h.");

	idn = "\xe2\x98\x83"; // <snowman>
	assert(DNS::isIDN(idn));
	assert(DNS::encodeIDN(idn) == "xn--n3h");

	std::string dn = "www.pocoproject.org";
	assert (!DNS::isIDN(dn));
	assert (DNS::encodeIDN(dn) == "www.pocoproject.org");
}


void DNSTest::testDecodeIDN()
{
	std::string enc("xn--dmin-moa0i.example");
	assert (DNS::isEncodedIDN(enc));
	assert (DNS::decodeIDN(enc) == "d\xc3\xb6m\xc3\xa4in.example"); // d"om"ain.example 

	enc = ".xn--dmin-moa0i.example";
	assert(DNS::isEncodedIDN(enc));
	assert(DNS::decodeIDN(enc) == ".d\xc3\xb6m\xc3\xa4in.example"); // .d"om"ain.example 

	enc = "xn--dmin-moa0i.example.";
	assert(DNS::isEncodedIDN(enc));
	assert(DNS::decodeIDN(enc) == "d\xc3\xb6m\xc3\xa4in.example."); // d"om"ain.example.

	enc = "xn--dmin-moa0i";
	assert(DNS::isEncodedIDN(enc));
	assert(DNS::decodeIDN(enc) == "d\xc3\xb6m\xc3\xa4in"); // d"om"ain

	enc = "foo.xn--bcdf-9na9b.example";
	assert (DNS::isEncodedIDN(enc));
	assert (DNS::decodeIDN(enc) == "foo.\xc3\xa2""bcd\xc3\xa9""f.example"); // foo.^abcd'ef.example

	enc = "xn--n3h.example";
	assert (DNS::isEncodedIDN(enc));
	assert (DNS::decodeIDN(enc) == "\xe2\x98\x83.example"); // <snowman>.example

	enc = "xn--n3h.";
	assert(DNS::isEncodedIDN(enc));
	assert(DNS::decodeIDN(enc) == "\xe2\x98\x83."); // <snowman>.

	enc = "xn--n3h";
	assert(DNS::isEncodedIDN(enc));
	assert(DNS::decodeIDN(enc) == "\xe2\x98\x83"); // <snowman>

	std::string dn = "www.pocoproject.org";
	assert (!DNS::isEncodedIDN(dn));
	assert (DNS::decodeIDN(dn) == "www.pocoproject.org");
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
	CppUnit_addTest(pSuite, DNSTest, testEncodeIDN);
	CppUnit_addTest(pSuite, DNSTest, testDecodeIDN);

	return pSuite;
}
