//
// IPAddressTest.cpp
//
// $Id: //poco/1.4/Net/testsuite/src/IPAddressTest.cpp#2 $
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "IPAddressTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Net/IPAddress.h"
#include "Poco/Net/NetException.h"


using Poco::Net::IPAddress;
using Poco::Net::InvalidAddressException;


IPAddressTest::IPAddressTest(const std::string& name): CppUnit::TestCase(name)
{
}


IPAddressTest::~IPAddressTest()
{
}


void IPAddressTest::testStringConv()
{
	IPAddress ia1("127.0.0.1");
	assert (ia1.family() == IPAddress::IPv4);
	assert (ia1.toString() == "127.0.0.1");
	
	IPAddress ia2("192.168.1.120");
	assert (ia2.family() == IPAddress::IPv4);
	assert (ia2.toString() == "192.168.1.120");
	
	IPAddress ia3("255.255.255.255");
	assert (ia3.family() == IPAddress::IPv4);
	assert (ia3.toString() == "255.255.255.255");

	IPAddress ia4("0.0.0.0");
	assert (ia4.family() == IPAddress::IPv4);
	assert (ia4.toString() == "0.0.0.0");

	IPAddress ia5(24,  IPAddress::IPv4);
	assert (ia5.family() == IPAddress::IPv4);
	assert (ia5.toString() == "255.255.255.0");
}


void IPAddressTest::testStringConv6()
{
#ifdef POCO_HAVE_IPv6
	IPAddress ia0("::1");
	assert (ia0.family() == IPAddress::IPv6);
	assert (ia0.toString() == "::1");

	IPAddress ia1("1080:0:0:0:8:600:200a:425c");
	assert (ia1.family() == IPAddress::IPv6);
	assert (ia1.toString() == "1080::8:600:200a:425c");
	
	IPAddress ia2("1080::8:600:200A:425C");
	assert (ia2.family() == IPAddress::IPv6);
	assert (ia2.toString() == "1080::8:600:200a:425c");
	
	IPAddress ia3("::192.168.1.120");
	assert (ia3.family() == IPAddress::IPv6);
	assert (ia3.toString() == "::192.168.1.120");

	IPAddress ia4("::ffff:192.168.1.120");
	assert (ia4.family() == IPAddress::IPv6);
	assert (ia4.toString() == "::ffff:192.168.1.120");

	IPAddress ia5(64, IPAddress::IPv6);
	assert (ia5.family() == IPAddress::IPv6);
	assert (ia5.toString() == "ffff:ffff:ffff:ffff::");

	IPAddress ia6(32, IPAddress::IPv6);
	assert (ia6.family() == IPAddress::IPv6);
	assert (ia6.toString() == "ffff:ffff::");
#endif
}


void IPAddressTest::testParse()
{
	IPAddress ip;
	assert (IPAddress::tryParse("192.168.1.120", ip));
	
	assert (!IPAddress::tryParse("192.168.1.280", ip));

	ip = IPAddress::parse("192.168.1.120");
	try
	{
		ip = IPAddress::parse("192.168.1.280");
		fail("bad address - must throw");
	}	
	catch (InvalidAddressException&)
	{
	}
}


void IPAddressTest::testClassification()
{
	IPAddress ip1("0.0.0.0"); // wildcard
	assert (ip1.isWildcard());
	assert (!ip1.isBroadcast());
	assert (!ip1.isLoopback());
	assert (!ip1.isMulticast());
	assert (!ip1.isUnicast());
	assert (!ip1.isLinkLocal());
	assert (!ip1.isSiteLocal());
	assert (!ip1.isWellKnownMC());
	assert (!ip1.isNodeLocalMC());
	assert (!ip1.isLinkLocalMC());
	assert (!ip1.isSiteLocalMC());
	assert (!ip1.isOrgLocalMC());
	assert (!ip1.isGlobalMC());
		
	IPAddress ip2("255.255.255.255"); // broadcast
	assert (!ip2.isWildcard());
	assert (ip2.isBroadcast());
	assert (!ip2.isLoopback());
	assert (!ip2.isMulticast());
	assert (!ip2.isUnicast());
	assert (!ip2.isLinkLocal());
	assert (!ip2.isSiteLocal());
	assert (!ip2.isWellKnownMC());
	assert (!ip2.isNodeLocalMC());
	assert (!ip2.isLinkLocalMC());
	assert (!ip2.isSiteLocalMC());
	assert (!ip2.isOrgLocalMC());
	assert (!ip2.isGlobalMC());
	
	IPAddress ip3("127.0.0.1"); // loopback
	assert (!ip3.isWildcard());
	assert (!ip3.isBroadcast());
	assert (ip3.isLoopback());
	assert (!ip3.isMulticast());
	assert (ip3.isUnicast());
	assert (!ip3.isLinkLocal());
	assert (!ip3.isSiteLocal());
	assert (!ip3.isWellKnownMC());
	assert (!ip3.isNodeLocalMC());
	assert (!ip3.isLinkLocalMC());
	assert (!ip3.isSiteLocalMC());
	assert (!ip3.isOrgLocalMC());
	assert (!ip3.isGlobalMC());

	IPAddress ip4("80.122.195.86"); // unicast
	assert (!ip4.isWildcard());
	assert (!ip4.isBroadcast());
	assert (!ip4.isLoopback());
	assert (!ip4.isMulticast());
	assert (ip4.isUnicast());
	assert (!ip4.isLinkLocal());
	assert (!ip4.isSiteLocal());
	assert (!ip4.isWellKnownMC());
	assert (!ip4.isNodeLocalMC());
	assert (!ip4.isLinkLocalMC());
	assert (!ip4.isSiteLocalMC());
	assert (!ip4.isOrgLocalMC());
	assert (!ip4.isGlobalMC());

	IPAddress ip5("169.254.1.20"); // link local unicast
	assert (!ip5.isWildcard());
	assert (!ip5.isBroadcast());
	assert (!ip5.isLoopback());
	assert (!ip5.isMulticast());
	assert (ip5.isUnicast());
	assert (ip5.isLinkLocal());
	assert (!ip5.isSiteLocal());
	assert (!ip5.isWellKnownMC());
	assert (!ip5.isNodeLocalMC());
	assert (!ip5.isLinkLocalMC());
	assert (!ip5.isSiteLocalMC());
	assert (!ip5.isOrgLocalMC());
	assert (!ip5.isGlobalMC());

	IPAddress ip6("192.168.1.120"); // site local unicast
	assert (!ip6.isWildcard());
	assert (!ip6.isBroadcast());
	assert (!ip6.isLoopback());
	assert (!ip6.isMulticast());
	assert (ip6.isUnicast());
	assert (!ip6.isLinkLocal());
	assert (ip6.isSiteLocal());
	assert (!ip6.isWellKnownMC());
	assert (!ip6.isNodeLocalMC());
	assert (!ip6.isLinkLocalMC());
	assert (!ip6.isSiteLocalMC());
	assert (!ip6.isOrgLocalMC());
	assert (!ip6.isGlobalMC());

	IPAddress ip7("10.0.0.138"); // site local unicast
	assert (!ip7.isWildcard());
	assert (!ip7.isBroadcast());
	assert (!ip7.isLoopback());
	assert (!ip7.isMulticast());
	assert (ip7.isUnicast());
	assert (!ip7.isLinkLocal());
	assert (ip7.isSiteLocal());
	assert (!ip7.isWellKnownMC());
	assert (!ip7.isNodeLocalMC());
	assert (!ip7.isLinkLocalMC());
	assert (!ip7.isSiteLocalMC());
	assert (!ip7.isOrgLocalMC());
	assert (!ip7.isGlobalMC());

	IPAddress ip8("172.18.1.200"); // site local unicast
	assert (!ip8.isWildcard());
	assert (!ip8.isBroadcast());
	assert (!ip8.isLoopback());
	assert (!ip8.isMulticast());
	assert (ip8.isUnicast());
	assert (!ip8.isLinkLocal());
	assert (ip8.isSiteLocal());
	assert (!ip8.isWellKnownMC());
	assert (!ip8.isNodeLocalMC());
	assert (!ip8.isLinkLocalMC());
	assert (!ip8.isSiteLocalMC());
	assert (!ip8.isOrgLocalMC());
	assert (!ip8.isGlobalMC());
}


void IPAddressTest::testMCClassification()
{
	IPAddress ip1("224.0.0.100"); // well-known multicast
	assert (!ip1.isWildcard());
	assert (!ip1.isBroadcast());
	assert (!ip1.isLoopback());
	assert (ip1.isMulticast());
	assert (!ip1.isUnicast());
	assert (!ip1.isLinkLocal());
	assert (!ip1.isSiteLocal());
	assert (ip1.isWellKnownMC());
	assert (!ip1.isNodeLocalMC());
	assert (ip1.isLinkLocalMC()); // well known are in the range of link local
	assert (!ip1.isSiteLocalMC());
	assert (!ip1.isOrgLocalMC());
	assert (!ip1.isGlobalMC());

	IPAddress ip2("224.1.0.100"); // link local unicast
	assert (!ip2.isWildcard());
	assert (!ip2.isBroadcast());
	assert (!ip2.isLoopback());
	assert (ip2.isMulticast());
	assert (!ip2.isUnicast());
	assert (!ip2.isLinkLocal());
	assert (!ip2.isSiteLocal());
	assert (!ip2.isWellKnownMC());
	assert (!ip2.isNodeLocalMC());
	assert (ip2.isLinkLocalMC());
	assert (!ip2.isSiteLocalMC());
	assert (!ip2.isOrgLocalMC());
	assert (ip2.isGlobalMC()); // link local fall in the range of global

	IPAddress ip3("239.255.0.100"); // site local unicast
	assert (!ip3.isWildcard());
	assert (!ip3.isBroadcast());
	assert (!ip3.isLoopback());
	assert (ip3.isMulticast());
	assert (!ip3.isUnicast());
	assert (!ip3.isLinkLocal());
	assert (!ip3.isSiteLocal());
	assert (!ip3.isWellKnownMC());
	assert (!ip3.isNodeLocalMC());
	assert (!ip3.isLinkLocalMC());
	assert (ip3.isSiteLocalMC());
	assert (!ip3.isOrgLocalMC());
	assert (!ip3.isGlobalMC());

	IPAddress ip4("239.192.0.100"); // org local unicast
	assert (!ip4.isWildcard());
	assert (!ip4.isBroadcast());
	assert (!ip4.isLoopback());
	assert (ip4.isMulticast());
	assert (!ip4.isUnicast());
	assert (!ip4.isLinkLocal());
	assert (!ip4.isSiteLocal());
	assert (!ip4.isWellKnownMC());
	assert (!ip4.isNodeLocalMC());
	assert (!ip4.isLinkLocalMC());
	assert (!ip4.isSiteLocalMC());
	assert (ip4.isOrgLocalMC());
	assert (!ip4.isGlobalMC());

	IPAddress ip5("224.2.127.254"); // global unicast
	assert (!ip5.isWildcard());
	assert (!ip5.isBroadcast());
	assert (!ip5.isLoopback());
	assert (ip5.isMulticast());
	assert (!ip5.isUnicast());
	assert (!ip5.isLinkLocal());
	assert (!ip5.isSiteLocal());
	assert (!ip5.isWellKnownMC());
	assert (!ip5.isNodeLocalMC());
	assert (ip5.isLinkLocalMC()); // link local fall in the range of global
	assert (!ip5.isSiteLocalMC());
	assert (!ip5.isOrgLocalMC());
	assert (ip5.isGlobalMC());
}


void IPAddressTest::testClassification6()
{
#ifdef POCO_HAVE_IPv6
	IPAddress ip1("::"); // wildcard
	assert (ip1.isWildcard());
	assert (!ip1.isBroadcast());
	assert (!ip1.isLoopback());
	assert (!ip1.isMulticast());
	assert (!ip1.isUnicast());
	assert (!ip1.isLinkLocal());
	assert (!ip1.isSiteLocal());
	assert (!ip1.isWellKnownMC());
	assert (!ip1.isNodeLocalMC());
	assert (!ip1.isLinkLocalMC());
	assert (!ip1.isSiteLocalMC());
	assert (!ip1.isOrgLocalMC());
	assert (!ip1.isGlobalMC());
		
	IPAddress ip3("::1"); // loopback
	assert (!ip3.isWildcard());
	assert (!ip3.isBroadcast());
	assert (ip3.isLoopback());
	assert (!ip3.isMulticast());
	assert (ip3.isUnicast());
	assert (!ip3.isLinkLocal());
	assert (!ip3.isSiteLocal());
	assert (!ip3.isWellKnownMC());
	assert (!ip3.isNodeLocalMC());
	assert (!ip3.isLinkLocalMC());
	assert (!ip3.isSiteLocalMC());
	assert (!ip3.isOrgLocalMC());
	assert (!ip3.isGlobalMC());

	IPAddress ip4("2001:0db8:85a3:0000:0000:8a2e:0370:7334"); // unicast
	assert (!ip4.isWildcard());
	assert (!ip4.isBroadcast());
	assert (!ip4.isLoopback());
	assert (!ip4.isMulticast());
	assert (ip4.isUnicast());
	assert (!ip4.isLinkLocal());
	assert (!ip4.isSiteLocal());
	assert (!ip4.isWellKnownMC());
	assert (!ip4.isNodeLocalMC());
	assert (!ip4.isLinkLocalMC());
	assert (!ip4.isSiteLocalMC());
	assert (!ip4.isOrgLocalMC());
	assert (!ip4.isGlobalMC());

	IPAddress ip5("fe80::21f:5bff:fec6:6707"); // link local unicast
	assert (!ip5.isWildcard());
	assert (!ip5.isBroadcast());
	assert (!ip5.isLoopback());
	assert (!ip5.isMulticast());
	assert (ip5.isUnicast());
	assert (ip5.isLinkLocal());
	assert (!ip5.isSiteLocal());
	assert (!ip5.isWellKnownMC());
	assert (!ip5.isNodeLocalMC());
	assert (!ip5.isLinkLocalMC());
	assert (!ip5.isSiteLocalMC());
	assert (!ip5.isOrgLocalMC());
	assert (!ip5.isGlobalMC());

	IPAddress ip10("fe80::12"); // link local unicast
	assert (!ip10.isWildcard());
	assert (!ip10.isBroadcast());
	assert (!ip10.isLoopback());
	assert (!ip10.isMulticast());
	assert (ip10.isUnicast());
	assert (ip10.isLinkLocal());
	assert (!ip10.isSiteLocal());
	assert (!ip10.isWellKnownMC());
	assert (!ip10.isNodeLocalMC());
	assert (!ip10.isLinkLocalMC());
	assert (!ip10.isSiteLocalMC());
	assert (!ip10.isOrgLocalMC());
	assert (!ip10.isGlobalMC());

	IPAddress ip6("fec0::21f:5bff:fec6:6707"); // site local unicast (RFC 4291)
	assert (!ip6.isWildcard());
	assert (!ip6.isBroadcast());
	assert (!ip6.isLoopback());
	assert (!ip6.isMulticast());
	assert (ip6.isUnicast());
	assert (!ip6.isLinkLocal());
	assert (ip6.isSiteLocal());
	assert (!ip6.isWellKnownMC());
	assert (!ip6.isNodeLocalMC());
	assert (!ip6.isLinkLocalMC());
	assert (!ip6.isSiteLocalMC());
	assert (!ip6.isOrgLocalMC());
	assert (!ip6.isGlobalMC());

	IPAddress ip7("fc00::21f:5bff:fec6:6707"); // site local unicast (RFC 4193)
	assert (!ip7.isWildcard());
	assert (!ip7.isBroadcast());
	assert (!ip7.isLoopback());
	assert (!ip7.isMulticast());
	assert (ip7.isUnicast());
	assert (!ip7.isLinkLocal());
	assert (ip7.isSiteLocal());
	assert (!ip7.isWellKnownMC());
	assert (!ip7.isNodeLocalMC());
	assert (!ip7.isLinkLocalMC());
	assert (!ip7.isSiteLocalMC());
	assert (!ip7.isOrgLocalMC());
	assert (!ip7.isGlobalMC());
#endif
}


void IPAddressTest::testMCClassification6()
{
#ifdef POCO_HAVE_IPv6
	IPAddress ip1("ff02:0:0:0:0:0:0:c"); // well-known link-local multicast
	assert (!ip1.isWildcard());
	assert (!ip1.isBroadcast());
	assert (!ip1.isLoopback());
	assert (ip1.isMulticast());
	assert (!ip1.isUnicast());
	assert (!ip1.isLinkLocal());
	assert (!ip1.isSiteLocal());
	assert (ip1.isWellKnownMC());
	assert (!ip1.isNodeLocalMC());
	assert (ip1.isLinkLocalMC()); 
	assert (!ip1.isSiteLocalMC());
	assert (!ip1.isOrgLocalMC());
	assert (!ip1.isGlobalMC());

	IPAddress ip2("ff01:0:0:0:0:0:0:FB"); // node-local unicast
	assert (!ip2.isWildcard());
	assert (!ip2.isBroadcast());
	assert (!ip2.isLoopback());
	assert (ip2.isMulticast());
	assert (!ip2.isUnicast());
	assert (!ip2.isLinkLocal());
	assert (!ip2.isSiteLocal());
	assert (ip2.isWellKnownMC());
	assert (ip2.isNodeLocalMC());
	assert (!ip2.isLinkLocalMC());
	assert (!ip2.isSiteLocalMC());
	assert (!ip2.isOrgLocalMC());
	assert (!ip2.isGlobalMC()); 

	IPAddress ip3("ff05:0:0:0:0:0:0:FB"); // site local unicast
	assert (!ip3.isWildcard());
	assert (!ip3.isBroadcast());
	assert (!ip3.isLoopback());
	assert (ip3.isMulticast());
	assert (!ip3.isUnicast());
	assert (!ip3.isLinkLocal());
	assert (!ip3.isSiteLocal());
	assert (ip3.isWellKnownMC());
	assert (!ip3.isNodeLocalMC());
	assert (!ip3.isLinkLocalMC());
	assert (ip3.isSiteLocalMC());
	assert (!ip3.isOrgLocalMC());
	assert (!ip3.isGlobalMC());

	IPAddress ip4("ff18:0:0:0:0:0:0:FB"); // org local unicast
	assert (!ip4.isWildcard());
	assert (!ip4.isBroadcast());
	assert (!ip4.isLoopback());
	assert (ip4.isMulticast());
	assert (!ip4.isUnicast());
	assert (!ip4.isLinkLocal());
	assert (!ip4.isSiteLocal());
	assert (!ip4.isWellKnownMC());
	assert (!ip4.isNodeLocalMC());
	assert (!ip4.isLinkLocalMC());
	assert (!ip4.isSiteLocalMC());
	assert (ip4.isOrgLocalMC());
	assert (!ip4.isGlobalMC());

	IPAddress ip5("ff1f:0:0:0:0:0:0:FB"); // global unicast
	assert (!ip5.isWildcard());
	assert (!ip5.isBroadcast());
	assert (!ip5.isLoopback());
	assert (ip5.isMulticast());
	assert (!ip5.isUnicast());
	assert (!ip5.isLinkLocal());
	assert (!ip5.isSiteLocal());
	assert (!ip5.isWellKnownMC());
	assert (!ip5.isNodeLocalMC());
	assert (!ip5.isLinkLocalMC()); 
	assert (!ip5.isSiteLocalMC());
	assert (!ip5.isOrgLocalMC());
	assert (ip5.isGlobalMC());
#endif
}


void IPAddressTest::testRelationals()
{
	IPAddress ip1("192.168.1.120");
	IPAddress ip2(ip1);
	IPAddress ip3;
	IPAddress ip4("10.0.0.138");
	
	assert (ip1 != ip4);
	assert (ip1 == ip2);
	assert (!(ip1 != ip2));
	assert (!(ip1 == ip4));
	assert (ip1 > ip4);
	assert (ip1 >= ip4);
	assert (ip4 < ip1);
	assert (ip4 <= ip1);
	assert (!(ip1 < ip4));
	assert (!(ip1 <= ip4));
	assert (!(ip4 > ip1));
	assert (!(ip4 >= ip1));
	
	ip3 = ip1;
	assert (ip1 == ip3);
	ip3 = ip4;
	assert (ip1 != ip3);
	assert (ip3 == ip4);
}


void IPAddressTest::testWildcard()
{
	IPAddress wildcard = IPAddress::wildcard();
	assert (wildcard.isWildcard());
	assert (wildcard.toString() == "0.0.0.0");
}


void IPAddressTest::testBroadcast()
{
	IPAddress broadcast = IPAddress::broadcast();
	assert (broadcast.isBroadcast());
	assert (broadcast.toString() == "255.255.255.255");
}


void IPAddressTest::testPrefixCons()
{
	IPAddress ia1(15, IPAddress::IPv4);
	assert(ia1.toString() == "255.254.0.0");

#ifdef POCO_HAVE_IPv6
	IPAddress ia2(62, IPAddress::IPv6);
	assert(ia2.toString() == "ffff:ffff:ffff:fffc::");
#endif
}


void IPAddressTest::testPrefixLen()
{
	IPAddress ia1(15, IPAddress::IPv4);
	assert(ia1.prefixLength() == 15);

	IPAddress ia2(16, IPAddress::IPv4);
	assert(ia2.prefixLength() == 16);

	IPAddress ia3(23, IPAddress::IPv4);
	assert(ia3.prefixLength() == 23);

	IPAddress ia4(24, IPAddress::IPv4);
	assert(ia4.prefixLength() == 24);

	IPAddress ia5(25, IPAddress::IPv4);
	assert(ia5.prefixLength() == 25);

#ifdef POCO_HAVE_IPv6
	IPAddress ia6(62, IPAddress::IPv6);
	assert(ia6.prefixLength() == 62);

	IPAddress ia7(63, IPAddress::IPv6);
	assert(ia7.prefixLength() == 63);

	IPAddress ia8(64, IPAddress::IPv6);
	assert(ia8.prefixLength() == 64);

	IPAddress ia9(65, IPAddress::IPv6);
	assert(ia9.prefixLength() == 65);
#endif
}


void IPAddressTest::testOperators()
{
	IPAddress ip("10.0.0.51");
	IPAddress mask(24, IPAddress::IPv4);

	IPAddress net = ip & mask;
	assert(net.toString() == "10.0.0.0");

	IPAddress host("0.0.0.51");
	assert((net | host) == ip);

	assert((~mask).toString() == "0.0.0.255");
}


void IPAddressTest::testRelationals6()
{
#ifdef POCO_HAVE_IPv6
#endif
}


void IPAddressTest::testByteOrderMacros()
{
	Poco::UInt16 a16 = 0xDEAD;
	assert (poco_ntoh_16(a16) == ntohs(a16));
	assert (poco_hton_16(a16) == htons(a16));
	Poco::UInt32 a32 = 0xDEADBEEF;
	assert (poco_ntoh_32(a32) == ntohl(a32));
	assert (poco_hton_32(a32) == htonl(a32));
}


void IPAddressTest::setUp()
{
}


void IPAddressTest::tearDown()
{
}


CppUnit::Test* IPAddressTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("IPAddressTest");

	CppUnit_addTest(pSuite, IPAddressTest, testStringConv);
	CppUnit_addTest(pSuite, IPAddressTest, testStringConv6);
	CppUnit_addTest(pSuite, IPAddressTest, testParse);
	CppUnit_addTest(pSuite, IPAddressTest, testClassification);
	CppUnit_addTest(pSuite, IPAddressTest, testMCClassification);
	CppUnit_addTest(pSuite, IPAddressTest, testClassification6);
	CppUnit_addTest(pSuite, IPAddressTest, testMCClassification6);
	CppUnit_addTest(pSuite, IPAddressTest, testRelationals);
	CppUnit_addTest(pSuite, IPAddressTest, testRelationals6);
	CppUnit_addTest(pSuite, IPAddressTest, testWildcard);
	CppUnit_addTest(pSuite, IPAddressTest, testBroadcast);
	CppUnit_addTest(pSuite, IPAddressTest, testPrefixCons);
	CppUnit_addTest(pSuite, IPAddressTest, testPrefixLen);
	CppUnit_addTest(pSuite, IPAddressTest, testOperators);
	CppUnit_addTest(pSuite, IPAddressTest, testByteOrderMacros);

	return pSuite;
}
