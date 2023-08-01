//
// ActiveDispatcher.cpp
//
// Library: Foundation
// Package: Threading
// Module:  ActiveObjects
//
// Copyright (c) 2006-2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/ActiveDispatcher.h"
#include "Poco/Notification.h"
#include "Poco/AutoPtr.h"


namespace Poco {


namespace
{
	class MethodNotification: public Notification
	{
	public:
		MethodNotification(ActiveRunnableBase::Ptr pRunnable):
			_pRunnable(pRunnable)
		{
		}
		
		ActiveRunnableBase::Ptr runnable() const
		{
			return _pRunnable;
		}
		
	private:
		ActiveRunnableBase::Ptr _pRunnable;
	};
	
	class StopNotification: public Notification
	{
	};
}


ActiveDispatcher::ActiveDispatcher()
{
}


ActiveDispatcher::ActiveDispatcher(Thread::Priority prio)
{
	_thread.setPriority(prio);
}


ActiveDispatcher::~ActiveDispatcher()
{
	try
	{
		stop();
	}
	catch (...)
	{
	}
}


void ActiveDispatcher::start(ActiveRunnableBase::Ptr pRunnable)
{
	poco_check_ptr (pRunnable);

    if (!_thread.isRunning())
    {
        _thread.start(*this);
    }

    _queue.enqueueNotification(new MethodNotification(pRunnable));
}


void ActiveDispatcher::cancel()
{
	_queue.clear();
}


void ActiveDispatcher::run()
{
	AutoPtr<Notification> pNf = _queue.waitDequeueNotification();
	while (pNf && !dynamic_cast<StopNotification*>(pNf.get()))
	{
		MethodNotification* pMethodNf = dynamic_cast<MethodNotification*>(pNf.get());
		poco_check_ptr (pMethodNf);
		ActiveRunnableBase::Ptr pRunnable = pMethodNf->runnable();
		pRunnable->duplicate(); // run will release
		pRunnable->run();
		pRunnable = 0;
		pNf = 0;
		pNf = _queue.waitDequeueNotification();
	}
}


void ActiveDispatcher::stop()
{
	_queue.clear();
	_queue.wakeUpAll();
	_queue.enqueueNotification(new StopNotification);
	_thread.join();
}


} // namespace Poco
