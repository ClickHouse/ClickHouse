//
// TCPServerTest.cpp
//
// $Id: //poco/1.4/Net/testsuite/src/TCPServerTest.cpp#1 $
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "TCPServerTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Net/TCPServer.h"
#include "Poco/Net/TCPServerConnection.h"
#include "Poco/Net/TCPServerConnectionFactory.h"
#include "Poco/Net/TCPServerParams.h"
#include "Poco/Net/StreamSocket.h"
#include "Poco/Net/ServerSocket.h"
#include "Poco/Thread.h"
#include <iostream>


using Poco::Net::TCPServer;
using Poco::Net::TCPServerConnection;
using Poco::Net::TCPServerConnectionFactory;
using Poco::Net::TCPServerConnectionFactoryImpl;
using Poco::Net::TCPServerParams;
using Poco::Net::StreamSocket;
using Poco::Net::ServerSocket;
using Poco::Net::SocketAddress;
using Poco::Thread;


namespace
{
	class EchoConnection: public TCPServerConnection
	{
	public:
		EchoConnection(const StreamSocket& s): TCPServerConnection(s)
		{
		}
		
		void run()
		{
			StreamSocket& ss = socket();
			try
			{
				char buffer[256];
				int n = ss.receiveBytes(buffer, sizeof(buffer));
				while (n > 0)
				{
					ss.sendBytes(buffer, n);
					n = ss.receiveBytes(buffer, sizeof(buffer));
				}
			}
			catch (Poco::Exception& exc)
			{
				std::cerr << "EchoConnection: " << exc.displayText() << std::endl;
			}
		}
	};
}


TCPServerTest::TCPServerTest(const std::string& name): CppUnit::TestCase(name)
{
}


TCPServerTest::~TCPServerTest()
{
}


void TCPServerTest::testOneConnection()
{
	TCPServer srv(new TCPServerConnectionFactoryImpl<EchoConnection>());
	srv.start();
	assert (srv.currentConnections() == 0);
	assert (srv.currentThreads() == 0);
	assert (srv.queuedConnections() == 0);
	assert (srv.totalConnections() == 0);
	
	SocketAddress sa("localhost", srv.socket().address().port());
	StreamSocket ss1(sa);
	std::string data("hello, world");
	ss1.sendBytes(data.data(), (int) data.size());
	char buffer[256];
	int n = ss1.receiveBytes(buffer, sizeof(buffer));
	assert (n > 0);
	assert (std::string(buffer, n) == data);
	assert (srv.currentConnections() == 1);
	assert (srv.currentThreads() == 1);
	assert (srv.queuedConnections() == 0);
	assert (srv.totalConnections() == 1);
	ss1.close();
	Thread::sleep(1000);
	assert (srv.currentConnections() == 0);
}


void TCPServerTest::testTwoConnections()
{
	TCPServer srv(new TCPServerConnectionFactoryImpl<EchoConnection>());
	srv.start();
	assert (srv.currentConnections() == 0);
	assert (srv.currentThreads() == 0);
	assert (srv.queuedConnections() == 0);
	assert (srv.totalConnections() == 0);
	
	SocketAddress sa("localhost", srv.socket().address().port());
	StreamSocket ss1(sa);
	StreamSocket ss2(sa);
	std::string data("hello, world");
	ss1.sendBytes(data.data(), (int) data.size());
	ss2.sendBytes(data.data(), (int) data.size());

	char buffer[256];
	int n = ss1.receiveBytes(buffer, sizeof(buffer));
	assert (n > 0);
	assert (std::string(buffer, n) == data);

	n = ss2.receiveBytes(buffer, sizeof(buffer));
	assert (n > 0);
	assert (std::string(buffer, n) == data);
	
	assert (srv.currentConnections() == 2);
	assert (srv.currentThreads() == 2);
	assert (srv.queuedConnections() == 0);
	assert (srv.totalConnections() == 2);
	ss1.close();
	Thread::sleep(1000);
	assert (srv.currentConnections() == 1);
	assert (srv.currentThreads() == 1);
	assert (srv.queuedConnections() == 0);
	assert (srv.totalConnections() == 2);
	ss2.close();

	Thread::sleep(1000);
	assert (srv.currentConnections() == 0);
}


void TCPServerTest::testMultiConnections()
{
	ServerSocket svs(0);
	TCPServerParams* pParams = new TCPServerParams;
	pParams->setMaxThreads(4);
	pParams->setMaxQueued(4);
	pParams->setThreadIdleTime(100);
	TCPServer srv(new TCPServerConnectionFactoryImpl<EchoConnection>(), svs, pParams);
	srv.start();
	assert (srv.currentConnections() == 0);
	assert (srv.currentThreads() == 0);
	assert (srv.maxThreads() >= 4);
	assert (srv.queuedConnections() == 0);
	assert (srv.totalConnections() == 0);
	
	SocketAddress sa("localhost", svs.address().port());
	StreamSocket ss1(sa);
	StreamSocket ss2(sa);
	StreamSocket ss3(sa);
	StreamSocket ss4(sa);
	std::string data("hello, world");
	ss1.sendBytes(data.data(), (int) data.size());
	ss2.sendBytes(data.data(), (int) data.size());
	ss3.sendBytes(data.data(), (int) data.size());
	ss4.sendBytes(data.data(), (int) data.size());

	char buffer[256];
	int n = ss1.receiveBytes(buffer, sizeof(buffer));
	assert (n > 0);
	assert (std::string(buffer, n) == data);

	n = ss2.receiveBytes(buffer, sizeof(buffer));
	assert (n > 0);
	assert (std::string(buffer, n) == data);

	n = ss3.receiveBytes(buffer, sizeof(buffer));
	assert (n > 0);
	assert (std::string(buffer, n) == data);

	n = ss4.receiveBytes(buffer, sizeof(buffer));
	assert (n > 0);
	assert (std::string(buffer, n) == data);
	
	assert (srv.currentConnections() == 4);
	assert (srv.currentThreads() == 4);
	assert (srv.queuedConnections() == 0);
	assert (srv.totalConnections() == 4);
	
	StreamSocket ss5(sa);
	Thread::sleep(200);
	assert (srv.queuedConnections() == 1);
	StreamSocket ss6(sa);
	Thread::sleep(200);
	assert (srv.queuedConnections() == 2);
	
	ss1.close();
	Thread::sleep(2000);
	assert (srv.currentConnections() == 4);
	assert (srv.currentThreads() == 4);
	assert (srv.queuedConnections() == 1);
	assert (srv.totalConnections() == 5);

	ss2.close();
	Thread::sleep(2000);
	assert (srv.currentConnections() == 4);
	assert (srv.currentThreads() == 4);
	assert (srv.queuedConnections() == 0);
	assert (srv.totalConnections() == 6);
	
	ss3.close();
	Thread::sleep(2000);
	assert (srv.currentConnections() == 3);
	assert (srv.currentThreads() == 3);
	assert (srv.queuedConnections() == 0);
	assert (srv.totalConnections() == 6);

	ss4.close();
	Thread::sleep(2000);
	assert (srv.currentConnections() == 2);
	assert (srv.currentThreads() == 2);
	assert (srv.queuedConnections() == 0);
	assert (srv.totalConnections() == 6);

	ss5.close();
	ss6.close();
	Thread::sleep(1000);
	assert (srv.currentConnections() == 0);
}

void TCPServerTest::testThreadCapacity(){
	ServerSocket svs(0);
	TCPServerParams* pParams = new TCPServerParams;
	pParams->setMaxThreads(64);
	TCPServer srv(new TCPServerConnectionFactoryImpl<EchoConnection>(), svs, pParams);
	srv.start();
	assert (srv.maxThreads() >= 64);
}



void TCPServerTest::setUp()
{
}


void TCPServerTest::tearDown()
{
}


CppUnit::Test* TCPServerTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("TCPServerTest");

	CppUnit_addTest(pSuite, TCPServerTest, testOneConnection);
	CppUnit_addTest(pSuite, TCPServerTest, testTwoConnections);
	CppUnit_addTest(pSuite, TCPServerTest, testMultiConnections);
	CppUnit_addTest(pSuite, TCPServerTest, testThreadCapacity);

	return pSuite;
}
