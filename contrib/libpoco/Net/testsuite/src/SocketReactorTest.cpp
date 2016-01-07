//
// SocketReactorTest.cpp
//
// $Id: //poco/1.4/Net/testsuite/src/SocketReactorTest.cpp#1 $
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "SocketReactorTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Net/SocketReactor.h"
#include "Poco/Net/SocketNotification.h"
#include "Poco/Net/SocketConnector.h"
#include "Poco/Net/SocketAcceptor.h"
#include "Poco/Net/ParallelSocketAcceptor.h"
#include "Poco/Net/StreamSocket.h"
#include "Poco/Net/ServerSocket.h"
#include "Poco/Net/SocketAddress.h"
#include "Poco/Observer.h"
#include "Poco/Exception.h"
#include <sstream>


using Poco::Net::SocketReactor;
using Poco::Net::SocketConnector;
using Poco::Net::SocketAcceptor;
using Poco::Net::ParallelSocketAcceptor;
using Poco::Net::StreamSocket;
using Poco::Net::ServerSocket;
using Poco::Net::SocketAddress;
using Poco::Net::SocketNotification;
using Poco::Net::ReadableNotification;
using Poco::Net::WritableNotification;
using Poco::Net::TimeoutNotification;
using Poco::Net::ShutdownNotification;
using Poco::Observer;
using Poco::IllegalStateException;


namespace
{
	class EchoServiceHandler
	{
	public:
		EchoServiceHandler(StreamSocket& socket, SocketReactor& reactor):
			_socket(socket),
			_reactor(reactor)
		{
			_reactor.addEventHandler(_socket, Observer<EchoServiceHandler, ReadableNotification>(*this, &EchoServiceHandler::onReadable));
		}
		
		~EchoServiceHandler()
		{
			_reactor.removeEventHandler(_socket, Observer<EchoServiceHandler, ReadableNotification>(*this, &EchoServiceHandler::onReadable));
		}
		
		void onReadable(ReadableNotification* pNf)
		{
			pNf->release();
			char buffer[8];
			int n = _socket.receiveBytes(buffer, sizeof(buffer));
			if (n > 0)
			{
				_socket.sendBytes(buffer, n);
			}
			else
			{
				_socket.shutdownSend();
				delete this;
			}
		}
		
	private:
		StreamSocket   _socket;
		SocketReactor& _reactor;
	};
	
	class ClientServiceHandler
	{
	public:
		ClientServiceHandler(StreamSocket& socket, SocketReactor& reactor):
			_socket(socket),
			_reactor(reactor),
			_or(*this, &ClientServiceHandler::onReadable),
			_ow(*this, &ClientServiceHandler::onWritable),
			_ot(*this, &ClientServiceHandler::onTimeout)
		{
			_timeout = false;
			_readableError = false;
			_writableError = false;
			_timeoutError = false;
			checkReadableObserverCount(0);
			_reactor.addEventHandler(_socket, _or);
			checkReadableObserverCount(1);
			checkWritableObserverCount(0);
			_reactor.addEventHandler(_socket, _ow);
			checkWritableObserverCount(1);
			checkTimeoutObserverCount(0);
			_reactor.addEventHandler(_socket, _ot);
			checkTimeoutObserverCount(1);

		}
		
		~ClientServiceHandler()
		{
		}

		void onReadable(ReadableNotification* pNf)
		{
			pNf->release();
			char buffer[32];
			int n = _socket.receiveBytes(buffer, sizeof(buffer));
			if (n > 0)
			{
				_str.write(buffer, n);
			}
			else
			{
				checkReadableObserverCount(1);
				_reactor.removeEventHandler(_socket, Observer<ClientServiceHandler, ReadableNotification>(*this, &ClientServiceHandler::onReadable));
				checkReadableObserverCount(0);
				if (_once || _data.size() >= 3072) _reactor.stop();
				_data += _str.str();
				delete this;
			}
		}
		
		void onWritable(WritableNotification* pNf)
		{
			pNf->release();
			checkWritableObserverCount(1);
			_reactor.removeEventHandler(_socket, Observer<ClientServiceHandler, WritableNotification>(*this, &ClientServiceHandler::onWritable));
			checkWritableObserverCount(0);
			std::string data(1024, 'x');
			_socket.sendBytes(data.data(), (int) data.length());
			_socket.shutdownSend();
		}
		
		void onTimeout(TimeoutNotification* pNf)
		{
			pNf->release();
			_timeout = true;
			if (_closeOnTimeout) 
			{
				_reactor.stop();
				delete this;
			}
		}
		
		static std::string data()
		{
			return _data;
		}
		
		static void resetData()
		{
			_data.clear();
		}
		
		static bool timeout()
		{
			return _timeout;
		}

		static bool getCloseOnTimeout()
		{
			return _closeOnTimeout;
		}
		
		static void setCloseOnTimeout(bool flag)
		{
			_closeOnTimeout = flag;
		}
		
		static bool readableError()
		{
			return _readableError;
		}
		
		static bool writableError()
		{
			return _writableError;
		}
		
		static bool timeoutError()
		{
			return _timeoutError;
		}

		static void setOnce(bool once = true)
		{
			_once = once;
		}

	private:
		void checkReadableObserverCount(std::size_t oro)
		{
			if (((oro == 0) && _reactor.hasEventHandler(_socket, _or)) ||
				((oro > 0) && !_reactor.hasEventHandler(_socket, _or)))
			{
				_readableError = true;
			}
		}

		void checkWritableObserverCount(std::size_t ow)
		{
			if (((ow == 0) && _reactor.hasEventHandler(_socket, _ow)) ||
				((ow > 0) && !_reactor.hasEventHandler(_socket, _ow)))
			{
				_writableError = true;
			}
		}

		void checkTimeoutObserverCount(std::size_t ot)
		{
			if (((ot == 0) && _reactor.hasEventHandler(_socket, _ot)) ||
				((ot > 0) && !_reactor.hasEventHandler(_socket, _ot)))
			{
				_timeoutError = true;
			}
		}

		StreamSocket                                         _socket;
		SocketReactor&                                       _reactor;
		Observer<ClientServiceHandler, ReadableNotification> _or;
		Observer<ClientServiceHandler, WritableNotification> _ow;
		Observer<ClientServiceHandler, TimeoutNotification>  _ot;
		std::stringstream                                    _str;
		static std::string                                   _data;
		static bool                                          _readableError;
		static bool                                          _writableError;
		static bool                                          _timeoutError;
		static bool                                          _timeout;
		static bool                                          _closeOnTimeout;
		static bool                                          _once;
	};
	
	
	std::string ClientServiceHandler::_data;
	bool ClientServiceHandler::_readableError = false;
	bool ClientServiceHandler::_writableError = false;
	bool ClientServiceHandler::_timeoutError = false;
	bool ClientServiceHandler::_timeout = false;
	bool ClientServiceHandler::_closeOnTimeout = false;
	bool ClientServiceHandler::_once = false;
	
	
	class FailConnector: public SocketConnector<ClientServiceHandler>
	{
	public:
		FailConnector(SocketAddress& address, SocketReactor& reactor):
			SocketConnector<ClientServiceHandler>(address, reactor),
			_failed(false),
			_shutdown(false)
		{
			reactor.addEventHandler(socket(), Observer<FailConnector, TimeoutNotification>(*this, &FailConnector::onTimeout));
			reactor.addEventHandler(socket(), Observer<FailConnector, ShutdownNotification>(*this, &FailConnector::onShutdown));
		}
		
		void onShutdown(ShutdownNotification* pNf)
		{
			pNf->release();
			_shutdown = true;
		}
		
		void onTimeout(TimeoutNotification* pNf)
		{
			pNf->release();
			_failed = true;
			reactor()->stop();
		}

		void onError(int error)
		{
			_failed = true;
			reactor()->stop();
		}
		
		bool failed() const
		{
			return _failed;
		}

		bool shutdown() const
		{
			return _shutdown;
		}
		
	private:
		bool _failed;
		bool _shutdown;
	};
}


SocketReactorTest::SocketReactorTest(const std::string& name): CppUnit::TestCase(name)
{
}


SocketReactorTest::~SocketReactorTest()
{
}


void SocketReactorTest::testSocketReactor()
{
	SocketAddress ssa;
	ServerSocket ss(ssa);
	SocketReactor reactor;
	SocketAcceptor<EchoServiceHandler> acceptor(ss, reactor);
	SocketAddress sa("localhost", ss.address().port());
	SocketConnector<ClientServiceHandler> connector(sa, reactor);
	ClientServiceHandler::setOnce(true);
	ClientServiceHandler::resetData();
	reactor.run();
	std::string data(ClientServiceHandler::data());
	assert (data.size() == 1024);
	assert (!ClientServiceHandler::readableError());
	assert (!ClientServiceHandler::writableError());
	assert (!ClientServiceHandler::timeoutError());
}


void SocketReactorTest::testSetSocketReactor()
{
	SocketAddress ssa;
	ServerSocket ss(ssa);
	SocketReactor reactor;
	SocketAcceptor<EchoServiceHandler> acceptor(ss);
	acceptor.setReactor(reactor);
	SocketAddress sa("localhost", ss.address().port());
	SocketConnector<ClientServiceHandler> connector(sa, reactor);
	ClientServiceHandler::setOnce(true);
	ClientServiceHandler::resetData();
	reactor.run();
	std::string data(ClientServiceHandler::data());
	assert(data.size() == 1024);
	assert(!ClientServiceHandler::readableError());
	assert(!ClientServiceHandler::writableError());
	assert(!ClientServiceHandler::timeoutError());
}


void SocketReactorTest::testParallelSocketReactor()
{
	SocketAddress ssa;
	ServerSocket ss(ssa);
	SocketReactor reactor;
	ParallelSocketAcceptor<EchoServiceHandler, SocketReactor> acceptor(ss, reactor);
	SocketAddress sa("localhost", ss.address().port());
	SocketConnector<ClientServiceHandler> connector1(sa, reactor);
	SocketConnector<ClientServiceHandler> connector2(sa, reactor);
	SocketConnector<ClientServiceHandler> connector3(sa, reactor);
	SocketConnector<ClientServiceHandler> connector4(sa, reactor);
	ClientServiceHandler::setOnce(false);
	ClientServiceHandler::resetData();
	reactor.run();
	std::string data(ClientServiceHandler::data());
	assert (data.size() == 4096);
	assert (!ClientServiceHandler::readableError());
	assert (!ClientServiceHandler::writableError());
	assert (!ClientServiceHandler::timeoutError());
}


void SocketReactorTest::testSocketConnectorFail()
{
	SocketReactor reactor;
	reactor.setTimeout(Poco::Timespan(3, 0));
	SocketAddress sa("192.168.168.192", 12345);
	FailConnector connector(sa, reactor);
	assert (!connector.failed());
	assert (!connector.shutdown());
	reactor.run();
	assert (connector.failed());
	assert (connector.shutdown());
}


void SocketReactorTest::testSocketConnectorTimeout()
{
	ClientServiceHandler::setCloseOnTimeout(true);
	
	SocketAddress ssa;
	ServerSocket ss(ssa);
	SocketReactor reactor;
	SocketAddress sa("localhost", ss.address().port());
	SocketConnector<ClientServiceHandler> connector(sa, reactor);
	reactor.run();
	assert (ClientServiceHandler::timeout());
}


void SocketReactorTest::setUp()
{
	ClientServiceHandler::setCloseOnTimeout(false);
}


void SocketReactorTest::tearDown()
{
}


CppUnit::Test* SocketReactorTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("SocketReactorTest");

	CppUnit_addTest(pSuite, SocketReactorTest, testSocketReactor);
	CppUnit_addTest(pSuite, SocketReactorTest, testParallelSocketReactor);
	CppUnit_addTest(pSuite, SocketReactorTest, testSocketConnectorFail);
	CppUnit_addTest(pSuite, SocketReactorTest, testSocketConnectorTimeout);

	return pSuite;
}
