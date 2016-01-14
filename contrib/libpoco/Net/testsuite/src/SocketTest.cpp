//
// SocketTest.cpp
//
// $Id: //poco/1.4/Net/testsuite/src/SocketTest.cpp#1 $
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "SocketTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "EchoServer.h"
#include "Poco/Net/StreamSocket.h"
#include "Poco/Net/ServerSocket.h"
#include "Poco/Net/SocketAddress.h"
#include "Poco/Net/NetException.h"
#include "Poco/Timespan.h"
#include "Poco/Stopwatch.h"
#include "Poco/Buffer.h"
#include "Poco/FIFOBuffer.h"
#include "Poco/Delegate.h"
#include <iostream>


using Poco::Net::Socket;
using Poco::Net::StreamSocket;
using Poco::Net::ServerSocket;
using Poco::Net::SocketAddress;
using Poco::Net::ConnectionRefusedException;
using Poco::Timespan;
using Poco::Stopwatch;
using Poco::TimeoutException;
using Poco::InvalidArgumentException;
using Poco::Buffer;
using Poco::FIFOBuffer;
using Poco::delegate;


SocketTest::SocketTest(const std::string& name): CppUnit::TestCase(name)
{
}


SocketTest::~SocketTest()
{
}


void SocketTest::testEcho()
{
	EchoServer echoServer;
	StreamSocket ss;
	ss.connect(SocketAddress("localhost", echoServer.port()));
	int n = ss.sendBytes("hello", 5);
	assert (n == 5);
	char buffer[256];
	n = ss.receiveBytes(buffer, sizeof(buffer));
	assert (n == 5);
	assert (std::string(buffer, n) == "hello");
	ss.close();
}


void SocketTest::testPoll()
{
	EchoServer echoServer;
	StreamSocket ss;
	ss.connect(SocketAddress("localhost", echoServer.port()));
	Stopwatch sw;
	sw.start();
	Timespan timeout(1000000);
	assert (!ss.poll(timeout, Socket::SELECT_READ));
	assert (sw.elapsed() >= 900000);
	sw.restart();
	assert (ss.poll(timeout, Socket::SELECT_WRITE));
	assert (sw.elapsed() < 100000);
	ss.sendBytes("hello", 5);
	char buffer[256];
	sw.restart();
	assert (ss.poll(timeout, Socket::SELECT_READ));
	assert (sw.elapsed() < 100000);
	int n = ss.receiveBytes(buffer, sizeof(buffer));
	assert (n == 5);
	assert (std::string(buffer, n) == "hello");
	ss.close();
}


void SocketTest::testAvailable()
{
	EchoServer echoServer;
	StreamSocket ss;
	ss.connect(SocketAddress("localhost", echoServer.port()));
	Timespan timeout(1000000);
	ss.sendBytes("hello", 5);
	char buffer[256];
	assert (ss.poll(timeout, Socket::SELECT_READ));
	int av = ss.available();
	assert (av > 0 && av <= 5);
	int n = ss.receiveBytes(buffer, sizeof(buffer));
	assert (n == 5);
	assert (std::string(buffer, n) == "hello");
	ss.close();
}


void SocketTest::testFIFOBuffer()
{
	Buffer<char> b(5);
	b[0] = 'h';
	b[1] = 'e';
	b[2] = 'l';
	b[3] = 'l';
	b[4] = 'o';

	FIFOBuffer f(5, true);

	f.readable += delegate(this, &SocketTest::onReadable);
	f.writable += delegate(this, &SocketTest::onWritable);

	assert(0 == _notToReadable);
	assert(0 == _readableToNot);
	assert(0 == _notToWritable);
	assert(0 == _writableToNot);
	f.write(b);
	assert(1 == _notToReadable);
	assert(0 == _readableToNot);
	assert(0 == _notToWritable);
	assert(1 == _writableToNot);

	EchoServer echoServer;
	StreamSocket ss;
	ss.connect(SocketAddress("localhost", echoServer.port()));
	int n = ss.sendBytes(f);
	assert (n == 5);
	assert(1 == _notToReadable);
	assert(1 == _readableToNot);
	assert(1 == _notToWritable);
	assert(1 == _writableToNot);
	assert (f.isEmpty());

	n = ss.receiveBytes(f);
	assert (n == 5);
	
	assert(2 == _notToReadable);
	assert(1 == _readableToNot);
	assert(1 == _notToWritable);
	assert(2 == _writableToNot);

	assert (f[0] == 'h');
	assert (f[1] == 'e');
	assert (f[2] == 'l');
	assert (f[3] == 'l');
	assert (f[4] == 'o');

	f.readable -= delegate(this, &SocketTest::onReadable);
	f.writable -= delegate(this, &SocketTest::onWritable);

	ss.close();
}


void SocketTest::testConnect()
{
	ServerSocket serv;
	serv.bind(SocketAddress());
	serv.listen();
	StreamSocket ss;
	Timespan timeout(250000);
	ss.connect(SocketAddress("localhost", serv.address().port()), timeout);
}


void SocketTest::testConnectRefused()
{
	ServerSocket serv;
	serv.bind(SocketAddress());
	serv.listen();
	Poco::UInt16 port = serv.address().port();
	serv.close();
	StreamSocket ss;
	Timespan timeout(250000);
	try
	{
		ss.connect(SocketAddress("localhost", port));
		fail("connection refused - must throw");
	}
	catch (ConnectionRefusedException&)
	{
	}
}


void SocketTest::testConnectRefusedNB()
{
	ServerSocket serv;
	serv.bind(SocketAddress());
	serv.listen();
	Poco::UInt16 port = serv.address().port();
	serv.close();
	StreamSocket ss;
	Timespan timeout(2, 0);
	try
	{
		ss.connect(SocketAddress("localhost", port), timeout);
		fail("connection refused - must throw");
	}
	catch (TimeoutException&)
	{
	}
	catch (ConnectionRefusedException&)
	{
	}
}


void SocketTest::testNonBlocking()
{
	EchoServer echoServer;
	StreamSocket ss;
	ss.connect(SocketAddress("localhost", echoServer.port()));
	ss.setBlocking(false);

	Timespan timeout(1000000);
	assert (ss.poll(timeout, Socket::SELECT_WRITE));
	int n = ss.sendBytes("hello", 5);
	assert (n == 5);

	char buffer[256];
	assert (ss.poll(timeout, Socket::SELECT_READ));
	n = ss.receiveBytes(buffer, sizeof(buffer));
	assert (n == 5);
	assert (std::string(buffer, n) == "hello");
	ss.close();
}



void SocketTest::testAddress()
{
	ServerSocket serv;
	serv.bind(SocketAddress());
	serv.listen();
	StreamSocket ss;
	ss.connect(SocketAddress("localhost", serv.address().port()));
	StreamSocket css = serv.acceptConnection();
	assert (css.peerAddress().host() == ss.address().host());
	assert (css.peerAddress().port() == ss.address().port());
}


void SocketTest::testAssign()
{
	ServerSocket serv;
	StreamSocket ss1;
	StreamSocket ss2;
	
	assert (ss1 != ss2);
	StreamSocket ss3(ss1);
	assert (ss1 == ss3);
	ss3 = ss2;
	assert (ss1 != ss3);
	assert (ss2 == ss3);
	
	try
	{
		ss1 = serv;
		fail("incompatible assignment - must throw");
	}
	catch (InvalidArgumentException&)
	{
	}
	
	try
	{
		StreamSocket ss4(serv);
		fail("incompatible assignment - must throw");
	}
	catch (InvalidArgumentException&)
	{
	}

	try
	{
		serv = ss1;
		fail("incompatible assignment - must throw");
	}
	catch (InvalidArgumentException&)
	{
	}
	
	try
	{
		ServerSocket serv2(ss1);
		fail("incompatible assignment - must throw");
	}
	catch (InvalidArgumentException&)
	{
	}
}


void SocketTest::testTimeout()
{
	EchoServer echoServer;
	StreamSocket ss;
	ss.connect(SocketAddress("localhost", echoServer.port()));
	
	Timespan timeout0 = ss.getReceiveTimeout();
	Timespan timeout(250000);
	ss.setReceiveTimeout(timeout);
	Timespan timeout1 = ss.getReceiveTimeout();
	std::cout << "original receive timeout:  " << timeout0.totalMicroseconds() << std::endl;
	std::cout << "requested receive timeout: " << timeout.totalMicroseconds() << std::endl;
	std::cout << "actual receive timeout:    " << timeout1.totalMicroseconds() << std::endl;
	
	// some socket implementations adjust the timeout value
	// assert (ss.getReceiveTimeout() == timeout);
	Stopwatch sw;
	try
	{
		char buffer[256];
		sw.start();
		ss.receiveBytes(buffer, sizeof(buffer));
		fail("nothing to receive - must timeout");
	}
	catch (TimeoutException&)
	{
	}
	assert (sw.elapsed() < 1000000);
	
	timeout0 = ss.getSendTimeout();
	ss.setSendTimeout(timeout);
	timeout1 = ss.getSendTimeout();
	std::cout << "original send timeout:  " << timeout0.totalMicroseconds() << std::endl;
	std::cout << "requested send timeout: " << timeout.totalMicroseconds() << std::endl;
	std::cout << "actual send timeout:    " << timeout1.totalMicroseconds() << std::endl;
	// assert (ss.getSendTimeout() == timeout);
}


void SocketTest::testBufferSize()
{
	EchoServer echoServer;
	SocketAddress sa("localhost", 1234);
	StreamSocket ss(sa.family());
	
	int osz = ss.getSendBufferSize();
	int rsz = 32000;
	ss.setSendBufferSize(rsz);
	int asz = ss.getSendBufferSize();
	std::cout << "original send buffer size:  " << osz << std::endl;
	std::cout << "requested send buffer size: " << rsz << std::endl;
	std::cout << "actual send buffer size:    " << asz << std::endl;
	
	osz = ss.getReceiveBufferSize();
	ss.setReceiveBufferSize(rsz);
	asz = ss.getReceiveBufferSize();
	std::cout << "original recv buffer size:  " << osz << std::endl;
	std::cout << "requested recv buffer size: " << rsz << std::endl;
	std::cout << "actual recv buffer size:    " << asz << std::endl;
}


void SocketTest::testOptions()
{
	EchoServer echoServer;
	StreamSocket ss;
	ss.connect(SocketAddress("localhost", echoServer.port()));

	ss.setLinger(true, 20);
	bool f;
	int  t;
	ss.getLinger(f, t);
	assert (f && t == 20);
	ss.setLinger(false, 0);
	ss.getLinger(f, t);
	assert (!f);
	
	ss.setNoDelay(true);
	assert (ss.getNoDelay());
	ss.setNoDelay(false);
	assert (!ss.getNoDelay());
	
	ss.setKeepAlive(true);
	assert (ss.getKeepAlive());
	ss.setKeepAlive(false);
	assert (!ss.getKeepAlive());
	
	ss.setOOBInline(true);
	assert (ss.getOOBInline());
	ss.setOOBInline(false);
	assert (!ss.getOOBInline());
}


void SocketTest::testSelect()
{
	Timespan timeout(250000);

	EchoServer echoServer;
	StreamSocket ss;
	ss.connect(SocketAddress("localhost", echoServer.port()));

	Socket::SocketList readList;
	Socket::SocketList writeList;
	Socket::SocketList exceptList;

	readList.push_back(ss);
	assert (Socket::select(readList, writeList, exceptList, timeout) == 0);
	assert (readList.empty());
	assert (writeList.empty());
	assert (exceptList.empty());
	
	ss.sendBytes("hello", 5);

	ss.poll(timeout, Socket::SELECT_READ);

	readList.push_back(ss);
	writeList.push_back(ss);
	assert (Socket::select(readList, writeList, exceptList, timeout) == 2);
	assert (!readList.empty());
	assert (!writeList.empty());
	assert (exceptList.empty());

	char buffer[256];
	int n = ss.receiveBytes(buffer, sizeof(buffer));
	assert (n == 5);
	assert (std::string(buffer, n) == "hello");
	ss.close();
}


void SocketTest::testSelect2()
{
	Timespan timeout(100000);

	EchoServer echoServer1;
	EchoServer echoServer2;
	StreamSocket ss1(SocketAddress("localhost", echoServer1.port()));
	StreamSocket ss2(SocketAddress("localhost", echoServer2.port()));
	
	Socket::SocketList readList;
	Socket::SocketList writeList;
	Socket::SocketList exceptList;

	readList.push_back(ss1);
	readList.push_back(ss2);
	assert (Socket::select(readList, writeList, exceptList, timeout) == 0);
	assert (readList.empty());
	assert (writeList.empty());
	assert (exceptList.empty());
	
	ss1.sendBytes("hello", 5);

	ss1.poll(timeout, Socket::SELECT_READ);

	readList.push_back(ss1);
	readList.push_back(ss2);
	assert (Socket::select(readList, writeList, exceptList, timeout) == 1);

	assert (readList.size() == 1);
	assert (readList[0] == ss1);
	assert (writeList.empty());
	assert (exceptList.empty());

	char buffer[256];
	int n = ss1.receiveBytes(buffer, sizeof(buffer));
	assert (n == 5);

	readList.clear();
	writeList.clear();
	exceptList.clear();
	writeList.push_back(ss1);
	writeList.push_back(ss2);
	assert (Socket::select(readList, writeList, exceptList, timeout) == 2);
	assert (readList.empty());
	assert (writeList.size() == 2);
	assert (writeList[0] == ss1);
	assert (writeList[1] == ss2);
	assert (exceptList.empty());

	ss1.close();
	ss2.close();
}


void SocketTest::testSelect3()
{
	Socket::SocketList readList;
	Socket::SocketList writeList;
	Socket::SocketList exceptList;
	Timespan timeout(1000);

	int rc = Socket::select(readList, writeList, exceptList, timeout);
	assert (rc == 0);
}


void SocketTest::onReadable(bool& b)
{
	if (b) ++_notToReadable;
	else ++_readableToNot;
};


void SocketTest::onWritable(bool& b)
{
	if (b) ++_notToWritable;
	else ++_writableToNot;
}


void SocketTest::setUp()
{
	_readableToNot = 0;
	_notToReadable = 0;
	_writableToNot = 0;
	_notToWritable = 0;
}


void SocketTest::tearDown()
{
}


CppUnit::Test* SocketTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("SocketTest");

	CppUnit_addTest(pSuite, SocketTest, testEcho);
	CppUnit_addTest(pSuite, SocketTest, testPoll);
	CppUnit_addTest(pSuite, SocketTest, testAvailable);
	CppUnit_addTest(pSuite, SocketTest, testFIFOBuffer);
	CppUnit_addTest(pSuite, SocketTest, testConnect);
	CppUnit_addTest(pSuite, SocketTest, testConnectRefused);
	CppUnit_addTest(pSuite, SocketTest, testConnectRefusedNB);
	CppUnit_addTest(pSuite, SocketTest, testNonBlocking);
	CppUnit_addTest(pSuite, SocketTest, testAddress);
	CppUnit_addTest(pSuite, SocketTest, testAssign);
	CppUnit_addTest(pSuite, SocketTest, testTimeout);
	CppUnit_addTest(pSuite, SocketTest, testBufferSize);
	CppUnit_addTest(pSuite, SocketTest, testOptions);
	CppUnit_addTest(pSuite, SocketTest, testSelect);
	CppUnit_addTest(pSuite, SocketTest, testSelect2);
	CppUnit_addTest(pSuite, SocketTest, testSelect3);

	return pSuite;
}
