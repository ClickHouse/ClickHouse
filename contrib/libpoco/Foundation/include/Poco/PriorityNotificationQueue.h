//
// PriorityNotificationQueue.h
//
// $Id: //poco/1.4/Foundation/include/Poco/PriorityNotificationQueue.h#1 $
//
// Library: Foundation
// Package: Notifications
// Module:  PriorityNotificationQueue
//
// Definition of the PriorityNotificationQueue class.
//
// Copyright (c) 2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_PriorityNotificationQueue_INCLUDED
#define Foundation_PriorityNotificationQueue_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Notification.h"
#include "Poco/Mutex.h"
#include "Poco/Event.h"
#include <map>
#include <deque>


namespace Poco {


class NotificationCenter;


class Foundation_API PriorityNotificationQueue
	/// A PriorityNotificationQueue object provides a way to implement asynchronous
	/// notifications. This is especially useful for sending notifications
	/// from one thread to another, for example from a background thread to 
	/// the main (user interface) thread. 
	///
	/// The PriorityNotificationQueue is quite similar to the NotificationQueue class.
	/// The only difference to NotificationQueue is that each Notification is tagged
	/// with a priority value. When inserting a Notification into the queue, the
	/// Notification is inserted according to the given priority value, with 
	/// lower priority values being inserted before higher priority
	/// values. Therefore, the lower the numerical priority value, the higher
	/// the actual notification priority. 
	///
	/// Notifications are dequeued in order of their priority.
	/// 
	/// The PriorityNotificationQueue can also be used to distribute work from
	/// a controlling thread to one or more worker threads. Each worker thread
	/// repeatedly calls waitDequeueNotification() and processes the
	/// returned notification. Special care must be taken when shutting
	/// down a queue with worker threads waiting for notifications.
	/// The recommended sequence to shut down and destroy the queue is to
	///   1. set a termination flag for every worker thread
	///   2. call the wakeUpAll() method
	///   3. join each worker thread
	///   4. destroy the notification queue.
{
public:
	PriorityNotificationQueue();
		/// Creates the PriorityNotificationQueue.

	~PriorityNotificationQueue();
		/// Destroys the PriorityNotificationQueue.

	void enqueueNotification(Notification::Ptr pNotification, int priority);
		/// Enqueues the given notification by adding it to
		/// the queue according to the given priority.
		/// Lower priority values are inserted before higher priority values.
		/// The queue takes ownership of the notification, thus
		/// a call like
		///     notificationQueue.enqueueNotification(new MyNotification, 1);
		/// does not result in a memory leak.
		
	Notification* dequeueNotification();
		/// Dequeues the next pending notification.
		/// Returns 0 (null) if no notification is available.
		/// The caller gains ownership of the notification and
		/// is expected to release it when done with it.
		///
		/// It is highly recommended that the result is immediately
		/// assigned to a Notification::Ptr, to avoid potential
		/// memory management issues.

	Notification* waitDequeueNotification();
		/// Dequeues the next pending notification.
		/// If no notification is available, waits for a notification
		/// to be enqueued. 
		/// The caller gains ownership of the notification and
		/// is expected to release it when done with it.
		/// This method returns 0 (null) if wakeUpAll()
		/// has been called by another thread.
		///
		/// It is highly recommended that the result is immediately
		/// assigned to a Notification::Ptr, to avoid potential
		/// memory management issues.

	Notification* waitDequeueNotification(long milliseconds);
		/// Dequeues the next pending notification.
		/// If no notification is available, waits for a notification
		/// to be enqueued up to the specified time.
		/// Returns 0 (null) if no notification is available.
		/// The caller gains ownership of the notification and
		/// is expected to release it when done with it.
		///
		/// It is highly recommended that the result is immediately
		/// assigned to a Notification::Ptr, to avoid potential
		/// memory management issues.

	void dispatch(NotificationCenter& notificationCenter);
		/// Dispatches all queued notifications to the given
		/// notification center.

	void wakeUpAll();
		/// Wakes up all threads that wait for a notification.
	
	bool empty() const;
		/// Returns true iff the queue is empty.
		
	int size() const;
		/// Returns the number of notifications in the queue.

	void clear();
		/// Removes all notifications from the queue.
		
	bool hasIdleThreads() const;	
		/// Returns true if the queue has at least one thread waiting 
		/// for a notification.
		
	static PriorityNotificationQueue& defaultQueue();
		/// Returns a reference to the default
		/// PriorityNotificationQueue.

protected:
	Notification::Ptr dequeueOne();
	
private:
	typedef std::multimap<int, Notification::Ptr> NfQueue;
	struct WaitInfo
	{
		Notification::Ptr pNf;
		Event nfAvailable;
	};
	typedef std::deque<WaitInfo*> WaitQueue;

	NfQueue           _nfQueue;
	WaitQueue         _waitQueue;
	mutable FastMutex _mutex;
};


} // namespace Poco


#endif // Foundation_PriorityNotificationQueue_INCLUDED
