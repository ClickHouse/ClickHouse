//
// TCPServerDispatcher.cpp
//
// $Id: //poco/1.4/Net/src/TCPServerDispatcher.cpp#1 $
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
#include <memory>


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
	_mutex.lock();
	++_rc;
	_mutex.unlock();
}


void TCPServerDispatcher::release()
{
	_mutex.lock();
	int rc = --_rc;
	_mutex.unlock();
	if (rc == 0) delete this;
}


void TCPServerDispatcher::run()
{
	AutoPtr<TCPServerDispatcher> guard(this, true); // ensure object stays alive

	int idleTime = (int) _pParams->getThreadIdleTime().totalMilliseconds();

	for (;;)
	{
		AutoPtr<Notification> pNf = _queue.waitDequeueNotification(idleTime);
		if (pNf)
		{
			TCPConnectionNotification* pCNf = dynamic_cast<TCPConnectionNotification*>(pNf.get());
			if (pCNf)
			{
				std::auto_ptr<TCPServerConnection> pConnection(_pConnectionFactory->createConnection(pCNf->socket()));
				poco_check_ptr(pConnection.get());
				beginConnection();
				pConnection->start();
				endConnection();
			}
		}
	
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
		_queue.enqueueNotification(new TCPConnectionNotification(socket));
		if (!_queue.hasIdleThreads() && _currentThreads < _pParams->getMaxThreads())
		{
			try
			{
				_threadPool.startWithPriority(_pParams->getThreadPriority(), *this, threadName);
				++_currentThreads;
			}
			catch (Poco::Exception&)
			{
				// no problem here, connection is already queued
				// and a new thread might be available later.
			}
		}
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
	FastMutex::ScopedLock lock(_mutex);
	
	return _currentThreads;
}

int TCPServerDispatcher::maxThreads() const
{
	FastMutex::ScopedLock lock(_mutex);
	
	return _threadPool.capacity();
}


int TCPServerDispatcher::totalConnections() const
{
	FastMutex::ScopedLock lock(_mutex);
	
	return _totalConnections;
}


int TCPServerDispatcher::currentConnections() const
{
	FastMutex::ScopedLock lock(_mutex);
	
	return _currentConnections;
}


int TCPServerDispatcher::maxConcurrentConnections() const
{
	FastMutex::ScopedLock lock(_mutex);
	
	return _maxConcurrentConnections;
}


int TCPServerDispatcher::queuedConnections() const
{
	return _queue.size();
}


int TCPServerDispatcher::refusedConnections() const
{
	FastMutex::ScopedLock lock(_mutex);
	
	return _refusedConnections;
}


void TCPServerDispatcher::beginConnection()
{
	FastMutex::ScopedLock lock(_mutex);
	
	++_totalConnections;
	++_currentConnections;
	if (_currentConnections > _maxConcurrentConnections)
		_maxConcurrentConnections = _currentConnections;
}


void TCPServerDispatcher::endConnection()
{
	FastMutex::ScopedLock lock(_mutex);

	--_currentConnections;
}


} } // namespace Poco::Net
