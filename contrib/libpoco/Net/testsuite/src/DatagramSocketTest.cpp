//
// DatagramSocketTest.cpp
//
// $Id: //poco/1.4/Net/testsuite/src/DatagramSocketTest.cpp#1 $
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DatagramSocketTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "UDPEchoServer.h"
#include "Poco/Net/DatagramSocket.h"
#include "Poco/Net/SocketAddress.h"
#include "Poco/Net/NetworkInterface.h"
#include "Poco/Net/NetException.h"
#include "Poco/Timespan.h"
#include "Poco/Stopwatch.h"


using Poco::Net::Socket;
using Poco::Net::DatagramSocket;
using Poco::Net::SocketAddress;
using Poco::Net::IPAddress;
#ifdef POCO_NET_HAS_INTERFACE
	using Poco::Net::NetworkInterface;
#endif
using Poco::Timespan;
using Poco::Stopwatch;
using Poco::TimeoutException;
using Poco::InvalidArgumentException;
using Poco::IOException;


DatagramSocketTest::DatagramSocketTest(const std::string& name): CppUnit::TestCase(name)
{
}


DatagramSocketTest::~DatagramSocketTest()
{
}


void DatagramSocketTest::testEcho()
{
	UDPEchoServer echoServer;
	DatagramSocket ss;
	char buffer[256];
	ss.connect(SocketAddress("localhost", echoServer.port()));
	int n = ss.sendBytes("hello", 5);
	assert (n == 5);
	n = ss.receiveBytes(buffer, sizeof(buffer));
	assert (n == 5);
	assert (std::string(buffer, n) == "hello");
	ss.close();
}


void DatagramSocketTest::testSendToReceiveFrom()
{
	UDPEchoServer echoServer(SocketAddress("localhost", 0));
	DatagramSocket ss;
	int n = ss.sendTo("hello", 5, SocketAddress("localhost", echoServer.port()));
	assert (n == 5);
	char buffer[256];
	SocketAddress sa;
	n = ss.receiveFrom(buffer, sizeof(buffer), sa);
	assert (sa.host() == echoServer.address().host());
	assert (sa.port() == echoServer.port());
	assert (n == 5);
	assert (std::string(buffer, n) == "hello");
	ss.close();
}


void DatagramSocketTest::testBroadcast()
{
	UDPEchoServer echoServer;
	DatagramSocket ss(IPAddress::IPv4);

#if defined(POCO_NET_HAS_INTERFACE) && (POCO_OS == POCO_OS_FREE_BSD)
	NetworkInterface ni = NetworkInterface::forName("em0");
	SocketAddress sa(ni.broadcastAddress(1), echoServer.port());
#else
	SocketAddress sa("255.255.255.255", echoServer.port());
#endif
	// not all socket implementations fail if broadcast option is not set
/*
	try
	{
		int n = ss.sendTo("hello", 5, sa);
		fail ("broadcast option not set - must throw");
	}
	catch (IOException&)
	{
	}
*/
	ss.setBroadcast(true);

#if (POCO_OS == POCO_OS_FREE_BSD)
	int opt = 1;
	poco_socklen_t len = sizeof(opt);
	ss.impl()->setRawOption(IPPROTO_IP, IP_ONESBCAST, (const char*) &opt, len);
	ss.impl()->getRawOption(IPPROTO_IP, IP_ONESBCAST, &opt, len);
	assert (opt == 1);
#endif

	int n = ss.sendTo("hello", 5, sa);
	assert (n == 5);
	char buffer[256] = { 0 };
	n = ss.receiveBytes(buffer, 5);
	assert (n == 5);
	assert (std::string(buffer, n) == "hello");
	ss.close();
}


void DatagramSocketTest::setUp()
{
}


void DatagramSocketTest::tearDown()
{
}


CppUnit::Test* DatagramSocketTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("DatagramSocketTest");

	CppUnit_addTest(pSuite, DatagramSocketTest, testEcho);
	CppUnit_addTest(pSuite, DatagramSocketTest, testSendToReceiveFrom);
#if (POCO_OS != POCO_OS_FREE_BSD) // works only with local net bcast and very randomly
	CppUnit_addTest(pSuite, DatagramSocketTest, testBroadcast);
#endif

	return pSuite;
}
