//
// SocketAddressTest.cpp
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "SocketAddressTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Net/SocketAddress.h"
#include "Poco/Net/NetException.h"


using Poco::Net::SocketAddress;
using Poco::Net::IPAddress;
using Poco::Net::InvalidAddressException;
using Poco::Net::HostNotFoundException;
using Poco::Net::ServiceNotFoundException;
using Poco::Net::NoAddressFoundException;
using Poco::Net::AddressFamilyMismatchException;
using Poco::InvalidArgumentException;


SocketAddressTest::SocketAddressTest(const std::string& name): CppUnit::TestCase(name)
{
}


SocketAddressTest::~SocketAddressTest()
{
}


void SocketAddressTest::testSocketAddress()
{
	SocketAddress wild;
	assert (wild.host().isWildcard());
	assert (wild.port() == 0);

	SocketAddress sa1("192.168.1.100", 100);
	assert (sa1.af() == AF_INET);
	assert (sa1.family() == SocketAddress::IPv4);
	assert (sa1.host().toString() == "192.168.1.100");
	assert (sa1.port() == 100);
	assert (sa1.toString() == "192.168.1.100:100");

	SocketAddress sa2("192.168.1.100", "100");
	assert (sa2.host().toString() == "192.168.1.100");
	assert (sa2.port() == 100);

#if !defined(_WIN32_WCE)
	SocketAddress sa3("192.168.1.100", "ftp");
	assert (sa3.host().toString() == "192.168.1.100");
	assert (sa3.port() == 21);
#endif

	try
	{
		SocketAddress sa3("192.168.1.100", "f00bar");
		fail("bad service name - must throw");
	}
	catch (ServiceNotFoundException&)
	{
	}

	SocketAddress sa4("pocoproject.org", 80);
	assert (sa4.host().toString() == "54.93.62.90");
	assert (sa4.port() == 80);

	try
	{
		SocketAddress sa5("192.168.2.260", 80);
		fail("invalid address - must throw");
	}
	catch (HostNotFoundException&)
	{
	}
	catch (NoAddressFoundException&)
	{
	}

	try
	{
		SocketAddress sa6("192.168.2.120", "80000");
		fail("invalid port - must throw");
	}
	catch (ServiceNotFoundException&)
	{
	}

	SocketAddress sa7("192.168.2.120:88");
	assert (sa7.host().toString() == "192.168.2.120");
	assert (sa7.port() == 88);

	SocketAddress sa8("[192.168.2.120]:88");
	assert (sa8.host().toString() == "192.168.2.120");
	assert (sa8.port() == 88);

	try
	{
		SocketAddress sa9("[192.168.2.260]");
		fail("invalid address - must throw");
	}
	catch (InvalidArgumentException&)
	{
	}

	try
	{
		SocketAddress sa9("[192.168.2.260:88");
		fail("invalid address - must throw");
	}
	catch (InvalidArgumentException&)
	{
	}

	SocketAddress sa10("www6.pocoproject.org", 80);
	assert (sa10.host().toString() == "54.93.62.90" || sa10.host().toString() == "2001:4801:7828:101:be76:4eff:fe10:1455");

	SocketAddress sa11(SocketAddress::IPv4, "www6.pocoproject.org", 80);
	assert (sa11.host().toString() == "54.93.62.90");

#ifdef POCO_HAVE_IPv6
	try
	{
		SocketAddress sa12(SocketAddress::IPv6, "www6.pocoproject.org", 80);
		assert (sa12.host().toString() == "2001:4801:7828:101:be76:4eff:fe10:1455");
	}
	catch (AddressFamilyMismatchException&)
	{
		// may happen if no IPv6 address is configured on the system
	}
#endif
}


void SocketAddressTest::testSocketRelationals()
{
	SocketAddress sa1("192.168.1.100", 100);
    SocketAddress sa2("192.168.1.100:100");
	assert (sa1 == sa2);

    SocketAddress sa3("192.168.1.101", "99");
	assert (sa2 < sa3);

	SocketAddress sa4("192.168.1.101", "102");
	assert (sa3 < sa4);
}


void SocketAddressTest::testSocketAddress6()
{
#ifdef POCO_HAVE_IPv6
	SocketAddress sa1("FE80::E6CE:8FFF:FE4A:EDD0", 100);
	assert (sa1.af() == AF_INET6);
	assert (sa1.family() == SocketAddress::IPv6);
	assert (sa1.host().toString() == "fe80::e6ce:8fff:fe4a:edd0");
	assert (sa1.port() == 100);
	assert (sa1.toString() == "[fe80::e6ce:8fff:fe4a:edd0]:100");

	SocketAddress sa2("[FE80::E6CE:8FFF:FE4A:EDD0]:100");
	assert (sa2.af() == AF_INET6);
	assert (sa2.family() == SocketAddress::IPv6);
	assert (sa2.host().toString() == "fe80::e6ce:8fff:fe4a:edd0");
	assert (sa2.port() == 100);
	assert (sa2.toString() == "[fe80::e6ce:8fff:fe4a:edd0]:100");
#endif
}


void SocketAddressTest::testSocketAddressUnixLocal()
{
#ifdef POCO_OS_FAMILY_UNIX
	SocketAddress sa1(SocketAddress::UNIX_LOCAL, "/tmp/sock1");
	assert (sa1.af() == AF_UNIX);
	assert (sa1.family() == SocketAddress::UNIX_LOCAL);
	assert (sa1.toString() == "/tmp/sock1");

	SocketAddress sa2(SocketAddress::UNIX_LOCAL, "/tmp/sock2");
	assert (sa1 != sa2);
	assert (sa1 < sa2);

	SocketAddress sa3(SocketAddress::UNIX_LOCAL, "/tmp/sock1");
	assert (sa1 == sa3);
	assert (!(sa1 < sa3));

	SocketAddress sa4("/tmp/sock1");
	assert (sa1 == sa4);
	assert (sa4.toString() == "/tmp/sock1");
#endif
}


void SocketAddressTest::setUp()
{
}


void SocketAddressTest::tearDown()
{
}


CppUnit::Test* SocketAddressTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("SocketAddressTest");

	CppUnit_addTest(pSuite, SocketAddressTest, testSocketAddress);
	CppUnit_addTest(pSuite, SocketAddressTest, testSocketRelationals);
	CppUnit_addTest(pSuite, SocketAddressTest, testSocketAddress6);
	CppUnit_addTest(pSuite, SocketAddressTest, testSocketAddressUnixLocal);

	return pSuite;
}
