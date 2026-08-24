//
// TimedNotificationQueue.cpp
//
// Library: Foundation
// Package: Notifications
// Module:  TimedNotificationQueue
//
// Copyright (c) 2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/TimedNotificationQueue.h"
#include "Poco/Notification.h"
#include <limits>


namespace Poco {


TimedNotificationQueue::TimedNotificationQueue()
{
}


TimedNotificationQueue::~TimedNotificationQueue()
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


void TimedNotificationQueue::enqueueNotification(Notification::Ptr pNotification, Timestamp timestamp)
{
	poco_check_ptr (pNotification);

	Timestamp tsNow;
	Clock clock;
	Timestamp::TimeDiff diff = timestamp - tsNow;
	clock += diff;

	FastMutex::ScopedLock lock(_mutex);
	_nfQueue.insert(NfQueue::value_type(clock, pNotification));
	_nfAvailable.set();
}


void TimedNotificationQueue::enqueueNotification(Notification::Ptr pNotification, Clock clock)
{
	poco_check_ptr (pNotification);

	FastMutex::ScopedLock lock(_mutex);
	_nfQueue.insert(NfQueue::value_type(clock, pNotification));
	_nfAvailable.set();
}


Notification* TimedNotificationQueue::dequeueNotification()
{
	FastMutex::ScopedLock lock(_mutex);

	NfQueue::iterator it = _nfQueue.begin();
	if (it != _nfQueue.end())
	{
		Clock::ClockDiff sleep = -it->first.elapsed();
		if (sleep <= 0)
		{
			Notification::Ptr pNf = it->second;
			_nfQueue.erase(it);
			return pNf.duplicate();
		}
	}
	return 0;
}


Notification* TimedNotificationQueue::waitDequeueNotification()
{
	for (;;)
	{
		_mutex.lock();
		NfQueue::iterator it = _nfQueue.begin();
		if (it != _nfQueue.end())
		{
			_mutex.unlock();
			Clock::ClockDiff sleep = -it->first.elapsed();
			if (sleep <= 0)
			{
				return dequeueOne(it).duplicate();
			}
			else if (!wait(sleep))
			{
				return dequeueOne(it).duplicate();
			}
			else continue;
		}
		else
		{
			_mutex.unlock();
		}
		_nfAvailable.wait();
	}
}


Notification* TimedNotificationQueue::waitDequeueNotification(long milliseconds)
{
	while (milliseconds >= 0)
	{
		_mutex.lock();
		NfQueue::iterator it = _nfQueue.begin();
		if (it != _nfQueue.end())
		{
			_mutex.unlock();
			Clock now;
			Clock::ClockDiff sleep = it->first - now;
			if (sleep <= 0)
			{
				return dequeueOne(it).duplicate();
			}
			else if (sleep <= 1000*Clock::ClockDiff(milliseconds))
			{
				if (!wait(sleep))
				{
					return dequeueOne(it).duplicate();
				}
				else 
				{
					milliseconds -= static_cast<long>((now.elapsed() + 999)/1000);
					continue;
				}
			}
		}
		else
		{
			_mutex.unlock();
		}
		if (milliseconds > 0)
		{
			Clock now;
			_nfAvailable.tryWait(milliseconds);
			milliseconds -= static_cast<long>((now.elapsed() + 999)/1000);
		}
		else return 0;
	}
	return 0;
}


bool TimedNotificationQueue::wait(Clock::ClockDiff interval)
{
	const Clock::ClockDiff MAX_SLEEP = 8*60*60*Clock::ClockDiff(1000000); // sleep at most 8 hours at a time
	while (interval > 0)
	{
		Clock now;
		Clock::ClockDiff sleep = interval <= MAX_SLEEP ? interval : MAX_SLEEP;
		if (_nfAvailable.tryWait(static_cast<long>((sleep + 999)/1000)))
			return true;
		interval -= now.elapsed();
	}
	return false;
}


bool TimedNotificationQueue::empty() const
{
	FastMutex::ScopedLock lock(_mutex);
	return _nfQueue.empty();
}

	
int TimedNotificationQueue::size() const
{
	FastMutex::ScopedLock lock(_mutex);
	return static_cast<int>(_nfQueue.size());
}


void TimedNotificationQueue::clear()
{
	FastMutex::ScopedLock lock(_mutex);
	_nfQueue.clear();	
}


Notification::Ptr TimedNotificationQueue::dequeueOne(NfQueue::iterator& it)
{
	FastMutex::ScopedLock lock(_mutex);
	Notification::Ptr pNf = it->second;
	_nfQueue.erase(it);
	return pNf;
}


} // namespace Poco
