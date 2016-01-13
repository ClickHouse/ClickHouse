//
// TCPServerTest.cpp
//
// $Id: //poco/1.4/NetSSL_OpenSSL/testsuite/src/TCPServerTest.cpp#2 $
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
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
#include "Poco/Net/SecureStreamSocket.h"
#include "Poco/Net/SecureServerSocket.h"
#include "Poco/Net/Context.h"
#include "Poco/Net/Session.h"
#include "Poco/Net/SSLManager.h"
#include "Poco/Util/Application.h"
#include "Poco/Util/AbstractConfiguration.h"
#include "Poco/Thread.h"
#include <iostream>


using Poco::Net::TCPServer;
using Poco::Net::TCPServerConnection;
using Poco::Net::TCPServerConnectionFactory;
using Poco::Net::TCPServerConnectionFactoryImpl;
using Poco::Net::TCPServerParams;
using Poco::Net::StreamSocket;
using Poco::Net::SecureStreamSocket;
using Poco::Net::SecureServerSocket;
using Poco::Net::SocketAddress;
using Poco::Net::Context;
using Poco::Net::Session;
using Poco::Net::SSLManager;
using Poco::Thread;
using Poco::Util::Application;


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
	SecureServerSocket svs(0);
	TCPServer srv(new TCPServerConnectionFactoryImpl<EchoConnection>(), svs);
	srv.start();
	assert (srv.currentConnections() == 0);
	assert (srv.currentThreads() == 0);
	assert (srv.queuedConnections() == 0);
	assert (srv.totalConnections() == 0);
	
	SocketAddress sa("localhost", svs.address().port());
	SecureStreamSocket ss1(sa);
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
	Thread::sleep(300);
	assert (srv.currentConnections() == 0);
}


void TCPServerTest::testTwoConnections()
{
	SecureServerSocket svs(0);
	TCPServer srv(new TCPServerConnectionFactoryImpl<EchoConnection>(), svs);
	srv.start();
	assert (srv.currentConnections() == 0);
	assert (srv.currentThreads() == 0);
	assert (srv.queuedConnections() == 0);
	assert (srv.totalConnections() == 0);
	
	SocketAddress sa("localhost", svs.address().port());
	SecureStreamSocket ss1(sa);
	SecureStreamSocket ss2(sa);
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
	Thread::sleep(300);
	assert (srv.currentConnections() == 1);
	assert (srv.currentThreads() == 1);
	assert (srv.queuedConnections() == 0);
	assert (srv.totalConnections() == 2);
	ss2.close();

	Thread::sleep(300);
	assert (srv.currentConnections() == 0);
}


void TCPServerTest::testMultiConnections()
{
	SecureServerSocket svs(0);
	TCPServerParams* pParams = new TCPServerParams;
	pParams->setMaxThreads(4);
	pParams->setMaxQueued(4);
	pParams->setThreadIdleTime(100);
	TCPServer srv(new TCPServerConnectionFactoryImpl<EchoConnection>(), svs, pParams);
	srv.start();
	assert (srv.currentConnections() == 0);
	assert (srv.currentThreads() == 0);
	assert (srv.queuedConnections() == 0);
	assert (srv.totalConnections() == 0);
	
	SocketAddress sa("localhost", svs.address().port());
	SecureStreamSocket ss1(sa);
	SecureStreamSocket ss2(sa);
	SecureStreamSocket ss3(sa);
	SecureStreamSocket ss4(sa);
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
	
	SecureStreamSocket ss5;
	ss5.setLazyHandshake();
	ss5.connect(sa);
	Thread::sleep(200);
	assert (srv.queuedConnections() == 1);
	SecureStreamSocket ss6;
	ss6.setLazyHandshake();
	ss6.connect(sa);
	Thread::sleep(200);
	assert (srv.queuedConnections() == 2);
	
	ss1.close();
	Thread::sleep(300);
	assert (srv.currentConnections() == 4);
	assert (srv.currentThreads() == 4);
	assert (srv.queuedConnections() == 1);
	assert (srv.totalConnections() == 5);

	ss2.close();
	Thread::sleep(300);
	assert (srv.currentConnections() == 4);
	assert (srv.currentThreads() == 4);
	assert (srv.queuedConnections() == 0);
	assert (srv.totalConnections() == 6);
	
	ss3.close();
	Thread::sleep(300);
	assert (srv.currentConnections() == 3);
	assert (srv.currentThreads() == 3);
	assert (srv.queuedConnections() == 0);
	assert (srv.totalConnections() == 6);

	ss4.close();
	Thread::sleep(300);
	assert (srv.currentConnections() == 2);
	assert (srv.currentThreads() == 2);
	assert (srv.queuedConnections() == 0);
	assert (srv.totalConnections() == 6);

	ss5.close();
	ss6.close();
	Thread::sleep(300);
	assert (srv.currentConnections() == 0);
}


void TCPServerTest::testReuseSocket()
{
	SecureServerSocket svs(0);
	TCPServer srv(new TCPServerConnectionFactoryImpl<EchoConnection>(), svs);
	srv.start();
	assert (srv.currentConnections() == 0);
	assert (srv.currentThreads() == 0);
	assert (srv.queuedConnections() == 0);
	assert (srv.totalConnections() == 0);
	
	SocketAddress sa("localhost", svs.address().port());
	SecureStreamSocket ss1(sa);
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
	Thread::sleep(300);
	assert (srv.currentConnections() == 0);

	ss1.connect(sa);
	ss1.sendBytes(data.data(), (int) data.size());
	n = ss1.receiveBytes(buffer, sizeof(buffer));
	assert (n > 0);
	assert (std::string(buffer, n) == data);
	assert (srv.currentConnections() == 1);
	assert (srv.queuedConnections() == 0);
	assert (srv.totalConnections() == 2);
	ss1.close();
	Thread::sleep(300);
	assert (srv.currentConnections() == 0);
}


void TCPServerTest::testReuseSession()
{
	// ensure OpenSSL machinery is fully setup
	Context::Ptr pDefaultServerContext = SSLManager::instance().defaultServerContext();
	Context::Ptr pDefaultClientContext = SSLManager::instance().defaultClientContext();
	
	Context::Ptr pServerContext = new Context(
		Context::SERVER_USE, 
		Application::instance().config().getString("openSSL.server.privateKeyFile"),
		Application::instance().config().getString("openSSL.server.privateKeyFile"),
		Application::instance().config().getString("openSSL.server.caConfig"),
		Context::VERIFY_NONE,
		9,
		true,
		"ALL:!ADH:!LOW:!EXP:!MD5:@STRENGTH");	
	pServerContext->enableSessionCache(true, "TestSuite");
	pServerContext->setSessionTimeout(10);
	pServerContext->setSessionCacheSize(1000);
	pServerContext->disableStatelessSessionResumption();
	
	SecureServerSocket svs(0, 64, pServerContext);
	TCPServer srv(new TCPServerConnectionFactoryImpl<EchoConnection>(), svs);
	srv.start();
	assert (srv.currentConnections() == 0);
	assert (srv.currentThreads() == 0);
	assert (srv.queuedConnections() == 0);
	assert (srv.totalConnections() == 0);
	
	Context::Ptr pClientContext = new Context(
		Context::CLIENT_USE, 
		Application::instance().config().getString("openSSL.client.privateKeyFile"),
		Application::instance().config().getString("openSSL.client.privateKeyFile"),
		Application::instance().config().getString("openSSL.client.caConfig"),
		Context::VERIFY_RELAXED,
		9,
		true,
		"ALL:!ADH:!LOW:!EXP:!MD5:@STRENGTH");
	pClientContext->enableSessionCache(true);
	
	SocketAddress sa("localhost", svs.address().port());
	SecureStreamSocket ss1(sa, pClientContext);
	assert (!ss1.sessionWasReused());
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
	
	Session::Ptr pSession = ss1.currentSession();
	
	ss1.close();
	Thread::sleep(300);
	assert (srv.currentConnections() == 0);

	ss1.useSession(pSession);
	ss1.connect(sa);
	assert (ss1.sessionWasReused());
	assert (ss1.currentSession() == pSession);
	ss1.sendBytes(data.data(), (int) data.size());
	n = ss1.receiveBytes(buffer, sizeof(buffer));
	assert (n > 0);
	assert (std::string(buffer, n) == data);
	assert (srv.currentConnections() == 1);
	assert (srv.queuedConnections() == 0);
	assert (srv.totalConnections() == 2);
	ss1.close();
	Thread::sleep(300);
	assert (srv.currentConnections() == 0);

	Thread::sleep(15000); // wait for session to expire
	pServerContext->flushSessionCache();
	
	ss1.useSession(pSession);
	ss1.connect(sa);
	assert (!ss1.sessionWasReused());
	assert (ss1.currentSession() != pSession);
	ss1.sendBytes(data.data(), (int) data.size());
	n = ss1.receiveBytes(buffer, sizeof(buffer));
	assert (n > 0);
	assert (std::string(buffer, n) == data);
	assert (srv.currentConnections() == 1);
	assert (srv.queuedConnections() == 0);
	assert (srv.totalConnections() == 3);
	ss1.close();
	Thread::sleep(300);
	assert (srv.currentConnections() == 0);
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
	CppUnit_addTest(pSuite, TCPServerTest, testReuseSocket);
	CppUnit_addTest(pSuite, TCPServerTest, testReuseSession);

	return pSuite;
}
