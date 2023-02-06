//
// PollSetTest.cpp
//
// Copyright (c) 2016, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "PollSetTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "EchoServer.h"
#include "Poco/Net/StreamSocket.h"
#include "Poco/Net/ServerSocket.h"
#include "Poco/Net/SocketAddress.h"
#include "Poco/Net/NetException.h"
#include "Poco/Net/PollSet.h"
#include "Poco/Stopwatch.h"


using Poco::Net::Socket;
using Poco::Net::StreamSocket;
using Poco::Net::ServerSocket;
using Poco::Net::SocketAddress;
using Poco::Net::ConnectionRefusedException;
using Poco::Net::PollSet;
using Poco::Timespan;
using Poco::Stopwatch;


PollSetTest::PollSetTest(const std::string& name): CppUnit::TestCase(name)
{
}


PollSetTest::~PollSetTest()
{
}


void PollSetTest::testPoll()
{
	EchoServer echoServer1;
	EchoServer echoServer2;
	StreamSocket ss1;
	StreamSocket ss2;

	ss1.connect(SocketAddress("127.0.0.1", echoServer1.port()));
	ss2.connect(SocketAddress("127.0.0.1", echoServer2.port()));

	PollSet ps;
	ps.add(ss1, PollSet::POLL_READ);

	// nothing readable
	Stopwatch sw;
	sw.start();
	Timespan timeout(1000000);
	assert (ps.poll(timeout).empty());
	assert (sw.elapsed() >= 900000);
	sw.restart();

	ps.add(ss2, PollSet::POLL_READ);

	// ss1 must be writable, if polled for
	ps.update(ss1, PollSet::POLL_READ | PollSet::POLL_WRITE);
	PollSet::SocketModeMap sm = ps.poll(timeout);
	assert (sm.find(ss1) != sm.end());
	assert (sm.find(ss2) == sm.end());
	assert (sm.find(ss1)->second == PollSet::POLL_WRITE);
	assert (sw.elapsed() < 100000);

	ps.update(ss1, PollSet::POLL_READ);

	ss1.sendBytes("hello", 5);
	char buffer[256];
	sw.restart();
	sm = ps.poll(timeout);
	assert (sm.find(ss1) != sm.end());
	assert (sm.find(ss2) == sm.end());
	assert (sm.find(ss1)->second == PollSet::POLL_READ);
	assert (sw.elapsed() < 100000);

	int n = ss1.receiveBytes(buffer, sizeof(buffer));
	assert (n == 5);
	assert (std::string(buffer, n) == "hello");


	ss2.sendBytes("HELLO", 5);
	sw.restart();
	sm = ps.poll(timeout);
	assert (sm.find(ss1) == sm.end());
	assert (sm.find(ss2) != sm.end());
	assert (sm.find(ss2)->second == PollSet::POLL_READ);
	assert (sw.elapsed() < 100000);

	n = ss2.receiveBytes(buffer, sizeof(buffer));
	assert (n == 5);
	assert (std::string(buffer, n) == "HELLO");

	ps.remove(ss2);

	ss2.sendBytes("HELLO", 5);
	sw.restart();
	sm = ps.poll(timeout);
	assert (sm.empty());

	n = ss2.receiveBytes(buffer, sizeof(buffer));
	assert (n == 5);
	assert (std::string(buffer, n) == "HELLO");

	ss1.close();
	ss2.close();
}


void PollSetTest::setUp()
{
}


void PollSetTest::tearDown()
{
}


CppUnit::Test* PollSetTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("PollSetTest");

	CppUnit_addTest(pSuite, PollSetTest, testPoll);

	return pSuite;
}
