//
// MulticastSocketTest.cpp
//
// $Id: //poco/1.4/Net/testsuite/src/MulticastSocketTest.cpp#1 $
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "MulticastSocketTest.h"


#ifdef POCO_NET_HAS_INTERFACE


#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "MulticastEchoServer.h"
#include "Poco/Net/MulticastSocket.h"
#include "Poco/Net/SocketAddress.h"
#include "Poco/Net/NetException.h"
#include "Poco/Timespan.h"
#include "Poco/Stopwatch.h"


using Poco::Net::Socket;
using Poco::Net::MulticastSocket;
using Poco::Net::SocketAddress;
using Poco::Net::IPAddress;
using Poco::Timespan;
using Poco::Stopwatch;
using Poco::TimeoutException;
using Poco::InvalidArgumentException;
using Poco::IOException;


MulticastSocketTest::MulticastSocketTest(const std::string& name): CppUnit::TestCase(name)
{
}


MulticastSocketTest::~MulticastSocketTest()
{
}


void MulticastSocketTest::testMulticast()
{
	MulticastEchoServer echoServer;
	MulticastSocket ms;
	int n = ms.sendTo("hello", 5, echoServer.group());
	assert (n == 5);
	char buffer[256];
	n = ms.receiveBytes(buffer, sizeof(buffer));
	assert (n == 5);
	assert (std::string(buffer, n) == "hello");
	ms.close();
}


void MulticastSocketTest::setUp()
{
}


void MulticastSocketTest::tearDown()
{
}


CppUnit::Test* MulticastSocketTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("MulticastSocketTest");
#if (POCO_OS != POCO_OS_FREE_BSD) // TODO
	CppUnit_addTest(pSuite, MulticastSocketTest, testMulticast);
#endif
	return pSuite;
}


#endif // POCO_NET_HAS_INTERFACE
