//
// NotificationQueue.cpp
//
// $Id: //poco/1.4/Foundation/samples/NotificationQueue/src/NotificationQueue.cpp#1 $
//
// This sample demonstrates the NotificationQueue, ThreadPool,
// FastMutex and ScopedLock classes.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Notification.h"
#include "Poco/NotificationQueue.h"
#include "Poco/ThreadPool.h"
#include "Poco/Thread.h"
#include "Poco/Runnable.h"
#include "Poco/Mutex.h"
#include "Poco/Random.h"
#include "Poco/AutoPtr.h"
#include <iostream>


using Poco::Notification;
using Poco::NotificationQueue;
using Poco::ThreadPool;
using Poco::Thread;
using Poco::Runnable;
using Poco::FastMutex;
using Poco::AutoPtr;


class WorkNotification: public Notification
	// The notification sent to worker threads.
{
public:
	typedef AutoPtr<WorkNotification> Ptr;
	
	WorkNotification(int data):
		_data(data)
	{
	}
	
	int data() const
	{
		return _data;
	}

private:
	int _data;
};


class Worker: public Runnable
	// A worker thread that gets work items
	// from a NotificationQueue.
{
public:
	Worker(const std::string& name, NotificationQueue& queue):
		_name(name),
		_queue(queue)
	{
	}
	
	void run()
	{
		Poco::Random rnd;
		for (;;)
		{
			Notification::Ptr pNf(_queue.waitDequeueNotification());
			if (pNf)
			{
				WorkNotification::Ptr pWorkNf = pNf.cast<WorkNotification>();
				if (pWorkNf)
				{
					{
						FastMutex::ScopedLock lock(_mutex);
						std::cout << _name << " got work notification " << pWorkNf->data() << std::endl;
					}
					Thread::sleep(rnd.next(200));
				}
			}
			else break;
		}
	}
	
private:
	std::string        _name;
	NotificationQueue& _queue;
	static FastMutex   _mutex;
};


FastMutex Worker::_mutex;


int main(int argc, char** argv)
{
	NotificationQueue queue;
	
	// create some worker threads
	Worker worker1("Worker 1", queue);
	Worker worker2("Worker 2", queue);
	Worker worker3("Worker 3", queue);

	// start worker threads
	ThreadPool::defaultPool().start(worker1);
	ThreadPool::defaultPool().start(worker2);
	ThreadPool::defaultPool().start(worker3);

	// distribute some work
	for (int i = 0; i < 50; ++i)
	{
		queue.enqueueNotification(new WorkNotification(i));
	}
	
	// wait until queue is empty and all threads are 
	// waiting for new work.
	while (!queue.empty()) Thread::sleep(200);
	Thread::sleep(500);
	
	// stop all worker threads
	queue.wakeUpAll();
	ThreadPool::defaultPool().joinAll();
	
	return 0;
}
