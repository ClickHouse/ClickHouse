//
// TCPServerDispatcher.cpp
//
// Library: Net
// Package: TCPServer
// Module:  TCPServerDispatcher
//
// Copyright (c) 2005-2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/TCPServerDispatcher.h"
#include "Poco/Net/TCPServerConnectionFactory.h"
#include "Poco/Notification.h"
#include "Poco/AutoPtr.h"
#include "Poco/ErrorHandler.h"
#include <memory>
#include <iostream>


using Poco::Notification;
using Poco::FastMutex;
using Poco::AutoPtr;


namespace Poco {
namespace Net {


class TCPConnectionNotification: public Notification
{
public:
	TCPConnectionNotification(const StreamSocket& socket):
		_socket(socket)
	{
	}
	
	~TCPConnectionNotification()
	{
	}
	
	const StreamSocket& socket() const
	{
		return _socket;
	}

private:
	StreamSocket _socket;
};


TCPServerDispatcher::TCPServerDispatcher(TCPServerConnectionFactory::Ptr pFactory, Poco::ThreadPool& threadPool, TCPServerParams::Ptr pParams):
	_rc(1),
	_pParams(pParams),
	_currentThreads(0),
	_totalConnections(0),
	_currentConnections(0),
	_maxConcurrentConnections(0),
	_refusedConnections(0),
	_stopped(false),
	_pConnectionFactory(pFactory),
	_threadPool(threadPool)
{
	poco_check_ptr (pFactory);

	if (!_pParams)
		_pParams = new TCPServerParams;
	
	if (_pParams->getMaxThreads() == 0)
		_pParams->setMaxThreads(threadPool.capacity());
}


TCPServerDispatcher::~TCPServerDispatcher()
{
}


void TCPServerDispatcher::duplicate()
{
	++_rc;
}


void TCPServerDispatcher::release()
{
	if (--_rc == 0) delete this;
}


void TCPServerDispatcher::run()
{
	AutoPtr<TCPServerDispatcher> guard(this); // ensure object stays alive

	int idleTime = (int) _pParams->getThreadIdleTime().totalMilliseconds();

	for (;;)
	{
		try
		{
			AutoPtr<Notification> pNf = _queue.waitDequeueNotification(idleTime);
			if (pNf && !_stopped)
			{
				TCPConnectionNotification* pCNf = dynamic_cast<TCPConnectionNotification*>(pNf.get());
				if (pCNf)
				{
					beginConnection();
					if (!_stopped)
					{
						std::unique_ptr<TCPServerConnection> pConnection(_pConnectionFactory->createConnection(pCNf->socket()));
						poco_check_ptr(pConnection.get());
						pConnection->start();
					}
					/// endConnection() should be called after destroying TCPServerConnection,
					/// otherwise currentConnections() could become zero while some connections are yet still alive.
					endConnection();
				}
			}
		}
		catch (Poco::Exception &exc) { ErrorHandler::handle(exc); }
		catch (std::exception &exc)  { ErrorHandler::handle(exc); }
		catch (...)                  { ErrorHandler::handle();    }
		FastMutex::ScopedLock lock(_mutex);
		if (_stopped || (_currentThreads > 1 && _queue.empty()))
		{
			--_currentThreads;
			break;
		}
	}
}


namespace
{
	static const std::string threadName("TCPServerConnection");
}

	
void TCPServerDispatcher::enqueue(const StreamSocket& socket)
{
	FastMutex::ScopedLock lock(_mutex);

	if (_queue.size() < _pParams->getMaxQueued())
	{
		if (!_queue.hasIdleThreads() && _currentThreads < _pParams->getMaxThreads())
		{
			try
			{
                this->duplicate();
				_threadPool.startWithPriority(_pParams->getThreadPriority(), *this, threadName);
				++_currentThreads;
			}
			catch (Poco::Exception& exc)
			{
                this->release();
				++_refusedConnections;
				std::cerr << "Got exception while starting thread for connection. Error code: "
						  << exc.code() << ", message: '" << exc.displayText() << "'" << std::endl;
				return;
			}
		}
		_queue.enqueueNotification(new TCPConnectionNotification(socket));
	}
	else
	{
		++_refusedConnections;
	}
}


void TCPServerDispatcher::stop()
{
	_stopped = true;
	_queue.clear();
	_queue.wakeUpAll();
}


int TCPServerDispatcher::currentThreads() const
{
	return _currentThreads;
}

int TCPServerDispatcher::maxThreads() const
{
	FastMutex::ScopedLock lock(_mutex);
	
	return _threadPool.capacity();
}


int TCPServerDispatcher::totalConnections() const
{
	return _totalConnections;
}


int TCPServerDispatcher::currentConnections() const
{
	return _currentConnections;
}


int TCPServerDispatcher::maxConcurrentConnections() const
{
	return _maxConcurrentConnections;
}


int TCPServerDispatcher::queuedConnections() const
{
	return _queue.size();
}


int TCPServerDispatcher::refusedConnections() const
{
	return _refusedConnections;
}


void TCPServerDispatcher::beginConnection()
{
	FastMutex::ScopedLock lock(_mutex);

	++_totalConnections;
	++_currentConnections;
	if (_currentConnections > _maxConcurrentConnections)
		_maxConcurrentConnections.store(_currentConnections);
}


void TCPServerDispatcher::endConnection()
{
	--_currentConnections;
}


} } // namespace Poco::Net
