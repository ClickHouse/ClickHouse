//
// NTPClientTest.cpp
//
// $Id: //poco/1.4/Net/testsuite/src/NTPClientTest.cpp#1 $
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "NTPClientTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Net/NTPClient.h"
#include "Poco/Net/NTPEventArgs.h"
#include "Poco/Net/SocketAddress.h"
#include "Poco/Net/NetException.h"
#include "Poco/AutoPtr.h"
#include "Poco/Delegate.h"
#include "Poco/DateTimeFormatter.h"
#include "Poco/DateTimeFormat.h"
#include <sstream>
#include <iostream>


using Poco::Net::NTPClient;
using Poco::Net::NTPEventArgs;
using Poco::Net::SocketAddress;
using Poco::Net::IPAddress;
using Poco::Net::HostNotFoundException;
using Poco::Delegate;
using Poco::AutoPtr;


NTPClientTest::NTPClientTest(const std::string& name): 
	CppUnit::TestCase(name),
	_ntpClient(IPAddress::IPv4)
{
}


NTPClientTest::~NTPClientTest()
{
}


void NTPClientTest::testTimeSync()
{
	assert(_ntpClient.request("pool.ntp.org") > 0);
}


void NTPClientTest::setUp()
{
	_ntpClient.response += Delegate<NTPClientTest, NTPEventArgs>(this, &NTPClientTest::onResponse);
}


void NTPClientTest::tearDown()
{
	_ntpClient.response -= Delegate<NTPClientTest, NTPEventArgs>(this, &NTPClientTest::onResponse);
}


void NTPClientTest::onResponse(const void* pSender, NTPEventArgs& args)
{
	std::ostringstream os;
	os << std::endl << "Received from " << args.hostName() << " [" << args.hostAddress() << "] with " 
		<< Poco::DateTimeFormatter::format(args.packet().referenceTime(), Poco::DateTimeFormat::ISO8601_FORMAT) << " reference typestamp" 
		<< std::endl;
	std::cout << os.str() << std::endl;
}


CppUnit::Test* NTPClientTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("NTPClientTest");

	CppUnit_addTest(pSuite, NTPClientTest, testTimeSync);

	return pSuite;
}
