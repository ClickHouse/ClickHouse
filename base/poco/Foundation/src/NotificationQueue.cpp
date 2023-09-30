//
// NotificationQueue.cpp
//
// Library: Foundation
// Package: Notifications
// Module:  NotificationQueue
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/NotificationQueue.h"
#include "Poco/NotificationCenter.h"
#include "Poco/Notification.h"
#include "Poco/SingletonHolder.h"


namespace Poco {


NotificationQueue::NotificationQueue()
{
}


NotificationQueue::~NotificationQueue()
{
	try
	{
		clear();
	}
	catch (...)
	{
		poco_unexpected();
	}
}


void NotificationQueue::enqueueNotification(Notification::Ptr pNotification)
{
	poco_check_ptr (pNotification);
	FastMutex::ScopedLock lock(_mutex);
	if (_waitQueue.empty())
	{
		_nfQueue.push_back(pNotification);
	}
	else
	{
		WaitInfo* pWI = _waitQueue.front();
		_waitQueue.pop_front();
		pWI->pNf = pNotification;
		pWI->nfAvailable.set();
	}	
}


void NotificationQueue::enqueueUrgentNotification(Notification::Ptr pNotification)
{
	poco_check_ptr (pNotification);
	FastMutex::ScopedLock lock(_mutex);
	if (_waitQueue.empty())
	{
		_nfQueue.push_front(pNotification);
	}
	else
	{
		WaitInfo* pWI = _waitQueue.front();
		_waitQueue.pop_front();
		pWI->pNf = pNotification;
		pWI->nfAvailable.set();
	}	
}


Notification* NotificationQueue::dequeueNotification()
{
	FastMutex::ScopedLock lock(_mutex);
	return dequeueOne().duplicate();
}


Notification* NotificationQueue::waitDequeueNotification()
{
	Notification::Ptr pNf;
	WaitInfo* pWI = 0;
	{
		FastMutex::ScopedLock lock(_mutex);
		pNf = dequeueOne();
		if (pNf) return pNf.duplicate();
		pWI = new WaitInfo;
		_waitQueue.push_back(pWI);
	}
	pWI->nfAvailable.wait();
	pNf = pWI->pNf;
	delete pWI;
	return pNf.duplicate();
}


Notification* NotificationQueue::waitDequeueNotification(long milliseconds)
{
	Notification::Ptr pNf;
	WaitInfo* pWI = 0;
	{
		FastMutex::ScopedLock lock(_mutex);
		pNf = dequeueOne();
		if (pNf) return pNf.duplicate();
		pWI = new WaitInfo;
		_waitQueue.push_back(pWI);
	}
	if (pWI->nfAvailable.tryWait(milliseconds))
	{
		pNf = pWI->pNf;
	}
	else
	{
		FastMutex::ScopedLock lock(_mutex);
		pNf = pWI->pNf;
		for (WaitQueue::iterator it = _waitQueue.begin(); it != _waitQueue.end(); ++it)
		{
			if (*it == pWI)
			{
				_waitQueue.erase(it);
				break;
			}
		}
	}
	delete pWI;
	return pNf.duplicate();
}


void NotificationQueue::dispatch(NotificationCenter& notificationCenter)
{
	FastMutex::ScopedLock lock(_mutex);
	Notification::Ptr pNf = dequeueOne();
	while (pNf)
	{
		notificationCenter.postNotification(pNf);
		pNf = dequeueOne();
	}
}


void NotificationQueue::wakeUpAll()
{
	FastMutex::ScopedLock lock(_mutex);
	for (WaitQueue::iterator it = _waitQueue.begin(); it != _waitQueue.end(); ++it)
	{
		(*it)->nfAvailable.set();
	}
	_waitQueue.clear();
}


bool NotificationQueue::empty() const
{
	FastMutex::ScopedLock lock(_mutex);
	return _nfQueue.empty();
}

	
int NotificationQueue::size() const
{
	FastMutex::ScopedLock lock(_mutex);
	return static_cast<int>(_nfQueue.size());
}


void NotificationQueue::clear()
{
	FastMutex::ScopedLock lock(_mutex);
	_nfQueue.clear();	
}


bool NotificationQueue::hasIdleThreads() const
{
	FastMutex::ScopedLock lock(_mutex);
	return !_waitQueue.empty();
}


Notification::Ptr NotificationQueue::dequeueOne()
{
	Notification::Ptr pNf;
	if (!_nfQueue.empty())
	{
		pNf = _nfQueue.front();
		_nfQueue.pop_front();
	}
	return pNf;
}


namespace
{
	static SingletonHolder<NotificationQueue> sh;
}


NotificationQueue& NotificationQueue::defaultQueue()
{
	return *sh.get();
}


} // namespace Poco
