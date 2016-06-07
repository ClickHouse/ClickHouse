//
// ICMPSocketTest.cpp
//
// $Id: //poco/1.4/Net/testsuite/src/ICMPSocketTest.cpp#1 $
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "ICMPSocketTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "UDPEchoServer.h"
#include "Poco/Net/ICMPSocket.h"
#include "Poco/Net/SocketAddress.h"
#include "Poco/Net/NetException.h"
#include "Poco/Timespan.h"
#include "Poco/Stopwatch.h"


using Poco::Net::Socket;
using Poco::Net::ICMPSocket;
using Poco::Net::SocketAddress;
using Poco::Net::IPAddress;
using Poco::Timespan;
using Poco::Stopwatch;
using Poco::TimeoutException;
using Poco::Net::ICMPException;


ICMPSocketTest::ICMPSocketTest(const std::string& name): CppUnit::TestCase(name)
{
}


ICMPSocketTest::~ICMPSocketTest()
{
}


void ICMPSocketTest::testAssign()
{
	ICMPSocket s1(IPAddress::IPv4);
	ICMPSocket s2(s1);
}

void ICMPSocketTest::testSendToReceiveFrom()
{
	ICMPSocket ss(IPAddress::IPv4);

	SocketAddress sa("www.appinf.com", 0);
	SocketAddress sr(sa);

	try
	{
		ss.receiveFrom(sa);
		fail("must throw");
	}
	catch(ICMPException&)
	{
	}
	catch(TimeoutException&)
	{
	}

	ss.sendTo(sa);
	ss.receiveFrom(sa);

	assert(sr.host().toString() == sa.host().toString());
	ss.close();
}


void ICMPSocketTest::setUp()
{
}


void ICMPSocketTest::tearDown()
{
}


CppUnit::Test* ICMPSocketTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("ICMPSocketTest");

	CppUnit_addTest(pSuite, ICMPSocketTest, testSendToReceiveFrom);
	CppUnit_addTest(pSuite, ICMPSocketTest, testAssign);

	return pSuite;
}
