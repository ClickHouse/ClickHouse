//
// ICMPClientTest.cpp
//
// $Id: //poco/1.4/Net/testsuite/src/ICMPClientTest.cpp#1 $
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "ICMPClientTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Net/ICMPSocket.h"
#include "Poco/Net/ICMPClient.h"
#include "Poco/Net/ICMPEventArgs.h"
#include "Poco/Net/SocketAddress.h"
#include "Poco/Net/NetException.h"
#include "Poco/AutoPtr.h"
#include "Poco/Delegate.h"
#include <sstream>
#include <iostream>


using Poco::Net::ICMPSocket;
using Poco::Net::ICMPClient;
using Poco::Net::ICMPEventArgs;
using Poco::Net::SocketAddress;
using Poco::Net::IPAddress;
using Poco::Net::HostNotFoundException;
using Poco::Delegate;
using Poco::AutoPtr;


ICMPClientTest::ICMPClientTest(const std::string& name): 
	CppUnit::TestCase(name),
	_icmpClient(IPAddress::IPv4)
{
}


ICMPClientTest::~ICMPClientTest()
{
}


void ICMPClientTest::testPing()
{
	assert(ICMPClient::pingIPv4("localhost") > 0);

	assert(_icmpClient.ping("localhost") > 0);
	assert(_icmpClient.ping("www.appinf.com", 4) > 0);

	// warning: may fail depending on the existence of the addresses at test site
	// if so, adjust accordingly (i.e. specify non-existent or unreachable IP addresses)
	assert(0 == _icmpClient.ping("192.168.243.1"));
	assert(0 == _icmpClient.ping("10.11.12.13"));
}


void ICMPClientTest::setUp()
{
	_icmpClient.pingBegin += Delegate<ICMPClientTest, ICMPEventArgs>(this, &ICMPClientTest::onBegin);
	_icmpClient.pingReply += Delegate<ICMPClientTest, ICMPEventArgs>(this, &ICMPClientTest::onReply);
	_icmpClient.pingError += Delegate<ICMPClientTest, ICMPEventArgs>(this, &ICMPClientTest::onError);
	_icmpClient.pingEnd   += Delegate<ICMPClientTest, ICMPEventArgs>(this, &ICMPClientTest::onEnd);
}


void ICMPClientTest::tearDown()
{
	_icmpClient.pingBegin -= Delegate<ICMPClientTest, ICMPEventArgs>(this, &ICMPClientTest::onBegin);
	_icmpClient.pingReply -= Delegate<ICMPClientTest, ICMPEventArgs>(this, &ICMPClientTest::onReply);
	_icmpClient.pingError -= Delegate<ICMPClientTest, ICMPEventArgs>(this, &ICMPClientTest::onError);
	_icmpClient.pingEnd   -= Delegate<ICMPClientTest, ICMPEventArgs>(this, &ICMPClientTest::onEnd);
}


void ICMPClientTest::onBegin(const void* pSender, ICMPEventArgs& args)
{
	std::ostringstream os;
	os << std::endl << "Pinging " << args.hostName() << " [" << args.hostAddress() << "] with " 
		<< args.dataSize() << " bytes of data:" 
		<< std::endl << "-------------------------------------------------------" << std::endl;
	std::cout << os.str() << std::endl;
}


void ICMPClientTest::onReply(const void* pSender, ICMPEventArgs& args)
{
	std::ostringstream os;
	os << "Reply from " << args.hostAddress()
		<< " bytes=" << args.dataSize() 
		<< " time=" << args.replyTime() << "ms"
		<< " TTL=" << args.ttl();
	std::cout << os.str() << std::endl;
}


void ICMPClientTest::onError(const void* pSender, ICMPEventArgs& args)
{
	std::ostringstream os;
	os << args.error();
	std::cerr << os.str() << std::endl;
}


void ICMPClientTest::onEnd(const void* pSender, ICMPEventArgs& args)
{
	std::ostringstream os;
	int received = args.received();
	os << std::endl << "--- Ping statistics for " << args.hostAddress() << " ---"
		<< std::endl << "Packets: Sent=" << args.sent() << ", Received=" << received
		<< " Lost=" << args.repetitions() - received << " (" << 100.0 - args.percent() << "% loss),"
		<< std::endl << "Approximate round trip times in milliseconds: " << std::endl
		<< "Minimum=" << args.minRTT() << "ms, Maximum=" << args.maxRTT()  
		<< "ms, Average=" << args.avgRTT() << "ms" 
		<< std::endl << "-----------------------------------------------" << std::endl;
	std::cout << os.str() << std::endl;
}


CppUnit::Test* ICMPClientTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("ICMPClientTest");

	CppUnit_addTest(pSuite, ICMPClientTest, testPing);

	return pSuite;
}
