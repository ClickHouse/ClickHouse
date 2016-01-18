//
// Event_POSIX.cpp
//
// $Id: //poco/1.4/Foundation/src/Event_POSIX.cpp#3 $
//
// Library: Foundation
// Package: Threading
// Module:  Event
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Event_POSIX.h"
#if defined(POCO_VXWORKS)
#include <timers.h>
#include <cstring>
#else
#include <sys/time.h>
#endif


namespace Poco {


EventImpl::EventImpl(bool autoReset): _auto(autoReset), _state(false)
{
#if defined(POCO_VXWORKS)
	// This workaround is for VxWorks 5.x where
	// pthread_mutex_init() won't properly initialize the mutex
	// resulting in a subsequent freeze in pthread_mutex_destroy()
	// if the mutex has never been used.
	std::memset(&_mutex, 0, sizeof(_mutex));
#endif
	if (pthread_mutex_init(&_mutex, NULL))
		throw SystemException("cannot create event (mutex)");
	if (pthread_cond_init(&_cond, NULL))
		throw SystemException("cannot create event (condition)");
}


EventImpl::~EventImpl()
{
	pthread_cond_destroy(&_cond);
	pthread_mutex_destroy(&_mutex);
}


void EventImpl::waitImpl()
{
	if (pthread_mutex_lock(&_mutex))
		throw SystemException("wait for event failed (lock)"); 
	while (!_state) 
	{
		if (pthread_cond_wait(&_cond, &_mutex))
		{
			pthread_mutex_unlock(&_mutex);
			throw SystemException("wait for event failed");
		}
	}
	if (_auto)
		_state = false;
	pthread_mutex_unlock(&_mutex);
}


bool EventImpl::waitImpl(long milliseconds)
{
	int rc = 0;
	struct timespec abstime;

#if defined(__VMS)
	struct timespec delta;
	delta.tv_sec  = milliseconds / 1000;
	delta.tv_nsec = (milliseconds % 1000)*1000000;
	pthread_get_expiration_np(&delta, &abstime);
#elif defined(POCO_VXWORKS)
	clock_gettime(CLOCK_REALTIME, &abstime);
	abstime.tv_sec  += milliseconds / 1000;
	abstime.tv_nsec += (milliseconds % 1000)*1000000;
	if (abstime.tv_nsec >= 1000000000)
	{
		abstime.tv_nsec -= 1000000000;
		abstime.tv_sec++;
	}
#else
	struct timeval tv;
	gettimeofday(&tv, NULL);
	abstime.tv_sec  = tv.tv_sec + milliseconds / 1000;
	abstime.tv_nsec = tv.tv_usec*1000 + (milliseconds % 1000)*1000000;
	if (abstime.tv_nsec >= 1000000000)
	{
		abstime.tv_nsec -= 1000000000;
		abstime.tv_sec++;
	}
#endif

	if (pthread_mutex_lock(&_mutex) != 0)
		throw SystemException("wait for event failed (lock)"); 
	while (!_state) 
	{
		if ((rc = pthread_cond_timedwait(&_cond, &_mutex, &abstime)))
		{
			if (rc == ETIMEDOUT) break;
			pthread_mutex_unlock(&_mutex);
			throw SystemException("cannot wait for event");
		}
	}
	if (rc == 0 && _auto) _state = false;
	pthread_mutex_unlock(&_mutex);
	return rc == 0;
}


} // namespace Poco
