//
// SocketStreamTest.cpp
//
// $Id: //poco/1.4/Net/testsuite/src/SocketStreamTest.cpp#1 $
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "SocketStreamTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "EchoServer.h"
#include "Poco/Net/SocketStream.h"
#include "Poco/Net/StreamSocket.h"
#include "Poco/Net/ServerSocket.h"
#include "Poco/Net/SocketAddress.h"
#include "Poco/Net/NetException.h"
#include "Poco/Timespan.h"
#include "Poco/Stopwatch.h"


using Poco::Net::Socket;
using Poco::Net::SocketStream;
using Poco::Net::StreamSocket;
using Poco::Net::ServerSocket;
using Poco::Net::SocketAddress;
using Poco::Net::ConnectionRefusedException;
using Poco::Timespan;
using Poco::Stopwatch;
using Poco::TimeoutException;
using Poco::InvalidArgumentException;


SocketStreamTest::SocketStreamTest(const std::string& name): CppUnit::TestCase(name)
{
}


SocketStreamTest::~SocketStreamTest()
{
}


void SocketStreamTest::testStreamEcho()
{
	EchoServer echoServer;
	StreamSocket ss;
	ss.connect(SocketAddress("localhost", echoServer.port()));
	SocketStream str(ss);
	str << "hello";
	assert (str.good());
	str.flush();
	assert (str.good());
	ss.shutdownSend();

	char buffer[5];
	str.read(buffer, sizeof(buffer));
	assert (str.good());
	assert (str.gcount() == 5);
	assert (std::string(buffer, 5) == "hello");

	ss.close();
}


void SocketStreamTest::testLargeStreamEcho()
{
	const int msgSize = 64000;
	EchoServer echoServer;
	StreamSocket ss;
	ss.connect(SocketAddress("localhost", echoServer.port()));
	SocketStream str(ss);
	ss.setSendBufferSize(msgSize);
	ss.setReceiveBufferSize(msgSize);
	std::string payload(msgSize, 'x');
	str << payload;
	assert (str.good());
	str.flush();
	assert (str.good());
	ss.shutdownSend();

	assert (str.gcount() == 0);
	char buffer[msgSize];
	str.read(buffer, sizeof(buffer));
	assert (str.good());
	assert (str.gcount() == msgSize);

	ss.close();
}


void SocketStreamTest::testEOF()
{
	StreamSocket ss;
	SocketStream str(ss);
	{
		EchoServer echoServer;

		ss.connect(SocketAddress("localhost", echoServer.port()));
		str << "hello";
		assert (str.good());
		str.flush();
		assert (str.good());
		ss.shutdownSend();

		char buffer[5];
		str.read(buffer, sizeof(buffer));
		assert (str.good());
		assert (str.gcount() == 5);
		assert (std::string(buffer, 5) == "hello");
	}
	
	int c = str.get();
	assert (c == -1);
	assert (str.eof());
	
	ss.close();
}


void SocketStreamTest::setUp()
{
}


void SocketStreamTest::tearDown()
{
}


CppUnit::Test* SocketStreamTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("SocketStreamTest");

	CppUnit_addTest(pSuite, SocketStreamTest, testStreamEcho);
	CppUnit_addTest(pSuite, SocketStreamTest, testLargeStreamEcho);
	CppUnit_addTest(pSuite, SocketStreamTest, testEOF);

	return pSuite;
}
