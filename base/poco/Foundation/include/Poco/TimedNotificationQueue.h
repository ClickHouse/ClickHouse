//
// TimedNotificationQueue.h
//
// Library: Foundation
// Package: Notifications
// Module:  TimedNotificationQueue
//
// Definition of the TimedNotificationQueue class.
//
// Copyright (c) 2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_TimedNotificationQueue_INCLUDED
#define Foundation_TimedNotificationQueue_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Notification.h"
#include "Poco/Mutex.h"
#include "Poco/Event.h"
#include "Poco/Timestamp.h"
#include "Poco/Clock.h"
#include <map>


namespace Poco {


class Foundation_API TimedNotificationQueue
	/// A TimedNotificationQueue object provides a way to implement timed, asynchronous
	/// notifications. This is especially useful for sending notifications
	/// from one thread to another, for example from a background thread to 
	/// the main (user interface) thread. 
	///
	/// The TimedNotificationQueue is quite similar to the NotificationQueue class.
	/// The only difference to NotificationQueue is that each Notification is tagged
	/// with a Timestamp. When inserting a Notification into the queue, the
	/// Notification is inserted according to the given Timestamp, with 
	/// lower Timestamp values being inserted before higher ones.
	///
	/// Notifications are dequeued in order of their timestamps.
	///
	/// TimedNotificationQueue has some restrictions regarding multithreaded use.
	/// While multiple threads may enqueue notifications, only one thread at a
	/// time may dequeue notifications from the queue.
	///
	/// If two threads try to dequeue a notification simultaneously, the results
	/// are undefined.
{
public:
	TimedNotificationQueue();
		/// Creates the TimedNotificationQueue.

	~TimedNotificationQueue();
		/// Destroys the TimedNotificationQueue.

	void enqueueNotification(Notification::Ptr pNotification, Timestamp timestamp);
		/// Enqueues the given notification by adding it to
		/// the queue according to the given timestamp.
		/// Lower timestamp values are inserted before higher ones.
		/// The queue takes ownership of the notification, thus
		/// a call like
		///     notificationQueue.enqueueNotification(new MyNotification, someTime);
		/// does not result in a memory leak.
		///
		/// The Timestamp is converted to an equivalent Clock value.

	void enqueueNotification(Notification::Ptr pNotification, Clock clock);
		/// Enqueues the given notification by adding it to
		/// the queue according to the given clock value.
		/// Lower clock values are inserted before higher ones.
		/// The queue takes ownership of the notification, thus
		/// a call like
		///     notificationQueue.enqueueNotification(new MyNotification, someTime);
		/// does not result in a memory leak.

	Notification* dequeueNotification();
		/// Dequeues the next pending notification with a timestamp
		/// less than or equal to the current time.
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

	bool empty() const;
		/// Returns true iff the queue is empty.
		
	int size() const;
		/// Returns the number of notifications in the queue.

	void clear();
		/// Removes all notifications from the queue.
		///
		/// Calling clear() while another thread executes one of
		/// the dequeue member functions will result in undefined
		/// behavior.

protected:
	typedef std::multimap<Clock, Notification::Ptr> NfQueue;
	Notification::Ptr dequeueOne(NfQueue::iterator& it);
	bool wait(Clock::ClockDiff interval);
	
private:
	NfQueue _nfQueue;
	Event   _nfAvailable;
	mutable FastMutex _mutex;
};


} // namespace Poco


#endif // Foundation_TimedNotificationQueue_INCLUDED
