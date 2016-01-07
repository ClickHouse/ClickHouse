//
// SocketReactor.cpp
//
// $Id: //poco/1.4/Net/src/SocketReactor.cpp#1 $
//
// Library: Net
// Package: Reactor
// Module:  SocketReactor
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/SocketReactor.h"
#include "Poco/Net/SocketNotification.h"
#include "Poco/Net/SocketNotifier.h"
#include "Poco/ErrorHandler.h"
#include "Poco/Thread.h"
#include "Poco/Exception.h"


using Poco::FastMutex;
using Poco::Exception;
using Poco::ErrorHandler;


namespace Poco {
namespace Net {


SocketReactor::SocketReactor():
	_stop(false),
	_timeout(DEFAULT_TIMEOUT),
	_pReadableNotification(new ReadableNotification(this)),
	_pWritableNotification(new WritableNotification(this)),
	_pErrorNotification(new ErrorNotification(this)),
	_pTimeoutNotification(new TimeoutNotification(this)),
	_pIdleNotification(new IdleNotification(this)),
	_pShutdownNotification(new ShutdownNotification(this)),
	_pThread(0)
{
}


SocketReactor::SocketReactor(const Poco::Timespan& timeout):
	_stop(false),
	_timeout(timeout),
	_pReadableNotification(new ReadableNotification(this)),
	_pWritableNotification(new WritableNotification(this)),
	_pErrorNotification(new ErrorNotification(this)),
	_pTimeoutNotification(new TimeoutNotification(this)),
	_pIdleNotification(new IdleNotification(this)),
	_pShutdownNotification(new ShutdownNotification(this)),
	_pThread(0)
{
}


SocketReactor::~SocketReactor()
{
}


void SocketReactor::run()
{
	_pThread = Thread::current();

	Socket::SocketList readable;
	Socket::SocketList writable;
	Socket::SocketList except;
	
	while (!_stop)
	{
		try
		{
			readable.clear();
			writable.clear();
			except.clear();
			int nSockets = 0;
			{
				FastMutex::ScopedLock lock(_mutex);
				for (EventHandlerMap::iterator it = _handlers.begin(); it != _handlers.end(); ++it)
				{
					if (it->second->accepts(_pReadableNotification))
					{
						readable.push_back(it->first);
						nSockets++;
					}
					if (it->second->accepts(_pWritableNotification))
					{
						writable.push_back(it->first);
						nSockets++;
					}
					if (it->second->accepts(_pErrorNotification))
					{
						except.push_back(it->first);
						nSockets++;
					}
				}
			}
			if (nSockets == 0)
			{
				onIdle();
				Thread::trySleep(_timeout.milliseconds());
			}
			else if (Socket::select(readable, writable, except, _timeout))
			{
				onBusy();

				for (Socket::SocketList::iterator it = readable.begin(); it != readable.end(); ++it)
					dispatch(*it, _pReadableNotification);
				for (Socket::SocketList::iterator it = writable.begin(); it != writable.end(); ++it)
					dispatch(*it, _pWritableNotification);
				for (Socket::SocketList::iterator it = except.begin(); it != except.end(); ++it)
					dispatch(*it, _pErrorNotification);
			}
			else onTimeout();
		}
		catch (Exception& exc)
		{
			ErrorHandler::handle(exc);
		}
		catch (std::exception& exc)
		{
			ErrorHandler::handle(exc);
		}
		catch (...)
		{
			ErrorHandler::handle();
		}
	}
	onShutdown();
}


void SocketReactor::stop()
{
	_stop = true;
}


void SocketReactor::wakeUp()
{
	if (_pThread) _pThread->wakeUp();
}


void SocketReactor::setTimeout(const Poco::Timespan& timeout)
{
	_timeout = timeout;
}


const Poco::Timespan& SocketReactor::getTimeout() const
{
	return _timeout;
}


void SocketReactor::addEventHandler(const Socket& socket, const Poco::AbstractObserver& observer)
{
	NotifierPtr pNotifier;
	{
		FastMutex::ScopedLock lock(_mutex);
		
		EventHandlerMap::iterator it = _handlers.find(socket);
		if (it == _handlers.end())
		{
			pNotifier = new SocketNotifier(socket);
			_handlers[socket] = pNotifier;
		}
		else pNotifier = it->second;
	}
	if (!pNotifier->hasObserver(observer))
		pNotifier->addObserver(this, observer);
}


bool SocketReactor::hasEventHandler(const Socket& socket, const Poco::AbstractObserver& observer)
{
	NotifierPtr pNotifier;
	{
		FastMutex::ScopedLock lock(_mutex);
	
		EventHandlerMap::iterator it = _handlers.find(socket);
		if (it != _handlers.end())
		{
			if (it->second->hasObserver(observer))
				return true;
		}
	}

	return false;
}


void SocketReactor::removeEventHandler(const Socket& socket, const Poco::AbstractObserver& observer)
{
	NotifierPtr pNotifier;
	{
		FastMutex::ScopedLock lock(_mutex);
	
		EventHandlerMap::iterator it = _handlers.find(socket);
		if (it != _handlers.end())
		{
			pNotifier = it->second;
			if (pNotifier->hasObserver(observer) && pNotifier->countObservers() == 1)
			{
				_handlers.erase(it);
			}
		}
	}
	if (pNotifier && pNotifier->hasObserver(observer))
	{
		pNotifier->removeObserver(this, observer);
	}

}


void SocketReactor::onTimeout()
{
	dispatch(_pTimeoutNotification);
}


void SocketReactor::onIdle()
{
	dispatch(_pIdleNotification);
}


void SocketReactor::onShutdown()
{
	dispatch(_pShutdownNotification);
}


void SocketReactor::onBusy()
{
}


void SocketReactor::dispatch(const Socket& socket, SocketNotification* pNotification)
{
	NotifierPtr pNotifier;
	{
		FastMutex::ScopedLock lock(_mutex);
		EventHandlerMap::iterator it = _handlers.find(socket);
		if (it != _handlers.end())
			pNotifier = it->second;
		else
			return;
	}
	dispatch(pNotifier, pNotification);
}


void SocketReactor::dispatch(SocketNotification* pNotification)
{
	std::vector<NotifierPtr> delegates;
	delegates.reserve(_handlers.size());
	{
		FastMutex::ScopedLock lock(_mutex);
		for (EventHandlerMap::iterator it = _handlers.begin(); it != _handlers.end(); ++it)
			delegates.push_back(it->second);
	}
	for (std::vector<NotifierPtr>::iterator it = delegates.begin(); it != delegates.end(); ++it)
	{
		dispatch(*it, pNotification);
	}
}


void SocketReactor::dispatch(NotifierPtr& pNotifier, SocketNotification* pNotification)
{
	try
	{
		pNotifier->dispatch(pNotification);
	}
	catch (Exception& exc)
	{
		ErrorHandler::handle(exc);
	}
	catch (std::exception& exc)
	{
		ErrorHandler::handle(exc);
	}
	catch (...)
	{
		ErrorHandler::handle();
	}
}


} } // namespace Poco::Net
