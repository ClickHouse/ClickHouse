//
// RawSocketTest.cpp
//
// $Id: //poco/1.4/Net/testsuite/src/RawSocketTest.cpp#1 $
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "RawSocketTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Net/RawSocket.h"
#include "Poco/Net/RawSocketImpl.h"
#include "Poco/Net/SocketAddress.h"
#include "Poco/Net/NetException.h"
#include "Poco/Timespan.h"
#include "Poco/Stopwatch.h"


using Poco::Net::Socket;
using Poco::Net::RawSocket;
using Poco::Net::RawSocketImpl;
using Poco::Net::SocketAddress;
using Poco::Net::IPAddress;
using Poco::Timespan;
using Poco::Stopwatch;
using Poco::TimeoutException;
using Poco::InvalidArgumentException;
using Poco::IOException;


RawSocketTest::RawSocketTest(const std::string& name): CppUnit::TestCase(name)
{
}


RawSocketTest::~RawSocketTest()
{
}


void RawSocketTest::testEchoIPv4()
{
	SocketAddress sa("localhost", 0);
	RawSocket rs(IPAddress::IPv4);
	rs.connect(sa);

	int n = rs.sendBytes("hello", 5);
	assert (5 == n);

	char buffer[256] = "";
	unsigned char* ptr = (unsigned char*) buffer;

	n = rs.receiveBytes(buffer, sizeof(buffer));
	int shift = ((buffer[0] & 0x0F) * 4);
	ptr += shift;

	assert (5 == (n - shift));
	assert ("hello" == std::string((char*)ptr, 5));

	rs.close(); 
}


void RawSocketTest::testSendToReceiveFromIPv4()
{
	RawSocket rs(IPAddress::IPv4);
	
	int n = rs.sendTo("hello", 5, SocketAddress("localhost", 0));
	assert (n == 5);

	char buffer[256] = "";
	unsigned char* ptr = (unsigned char*) buffer;
	SocketAddress sa;
	n = rs.receiveFrom(buffer, sizeof(buffer), sa);
	int shift = ((buffer[0] & 0x0F) * 4);
	ptr += shift;

	assert ((n - shift) == 5);
	assert ("hello" == std::string((char*)ptr, 5));
	rs.close();
}


void RawSocketTest::setUp()
{
}


void RawSocketTest::tearDown()
{
}


CppUnit::Test* RawSocketTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("RawSocketTest");

	CppUnit_addTest(pSuite, RawSocketTest, testEchoIPv4);
	CppUnit_addTest(pSuite, RawSocketTest, testSendToReceiveFromIPv4);

	return pSuite;
}
