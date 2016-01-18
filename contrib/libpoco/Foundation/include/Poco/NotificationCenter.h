//
// NotificationCenter.h
//
// $Id: //poco/1.4/Foundation/include/Poco/NotificationCenter.h#2 $
//
// Library: Foundation
// Package: Notifications
// Module:  NotificationCenter
//
// Definition of the NotificationCenter class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_NotificationCenter_INCLUDED
#define Foundation_NotificationCenter_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Notification.h"
#include "Poco/Mutex.h"
#include "Poco/SharedPtr.h"
#include <vector>
#include <cstddef>


namespace Poco {


class AbstractObserver;


class Foundation_API NotificationCenter
	/// A NotificationCenter is essentially a notification dispatcher. 
	/// It notifies all observers of notifications meeting specific criteria.
	/// This information is encapsulated in Notification objects.
	/// Client objects register themselves with the notification center as observers of 
	/// specific notifications posted by other objects. When an event occurs, an object 
	/// posts an appropriate notification to the notification center. The notification  
	/// center invokes the registered method on each matching observer, passing the notification 
	/// as argument.
	///
	/// The order in which observers receive notifications is undefined. 
	/// It is possible for the posting object and the observing object to be the same.
	/// The NotificationCenter delivers notifications to observers synchronously. 
	/// In other words the postNotification() method does not return until all observers have 
	/// received and processed the notification. 
	/// If an observer throws an exception while handling a notification, the NotificationCenter
	/// stops dispatching the notification and postNotification() rethrows the exception.
	///
	/// In a multithreaded scenario, notifications are always delivered in the thread in which the 
	/// notification was posted, which may not be the same thread in which an observer registered itself.
	///
	/// The NotificationCenter class is basically a C++ implementation of the NSNotificationCenter class
	/// found in Apple's Cocoa (or OpenStep).
	///
	/// While handling a notification, an observer can unregister itself from the notification center,
	/// or it can register or unregister other observers. Observers added during a dispatch cycle
	/// will not receive the current notification.
	///
	/// The method receiving the notification must be implemented as
	///     void handleNotification(MyNotification* pNf);
	/// The handler method gets co-ownership of the Notification object
	/// and must release it when done. This is best done with an AutoPtr:
	///     void MyClass::handleNotification(MyNotification* pNf)
	///     {
	///         AutoPtr<MyNotification> nf(pNf);
	///         ...
	///     }
	///
	/// Alternatively, the NObserver class template can be used to register a callback
	/// method. In this case, the callback method receives the Notification in an
	/// AutoPtr and thus does not have to deal with object ownership issues:
	///     void MyClass::handleNotification(const AutoPtr<MyNotification>& pNf)
	///     {
	///         ...
	///     }
{
public:
	NotificationCenter();
		/// Creates the NotificationCenter.

	~NotificationCenter();
		/// Destroys the NotificationCenter.

	void addObserver(const AbstractObserver& observer);
		/// Registers an observer with the NotificationCenter.
		/// Usage:
		///     Observer<MyClass, MyNotification> obs(*this, &MyClass::handleNotification);
		///     notificationCenter.addObserver(obs);
		///
		/// Alternatively, the NObserver template class can be used instead of Observer.

	void removeObserver(const AbstractObserver& observer);
		/// Unregisters an observer with the NotificationCenter.

	bool hasObserver(const AbstractObserver& observer) const;
		/// Returns true if the observer is registered with this NotificationCenter.

	void postNotification(Notification::Ptr pNotification);
		/// Posts a notification to the NotificationCenter.
		/// The NotificationCenter then delivers the notification
		/// to all interested observers.
		/// If an observer throws an exception, dispatching terminates
		/// and the exception is rethrown to the caller.
		/// Ownership of the notification object is claimed and the
		/// notification is released before returning. Therefore,
		/// a call like
		///    notificationCenter.postNotification(new MyNotification);
		/// does not result in a memory leak.

	bool hasObservers() const;
		/// Returns true iff there is at least one registered observer.
		///
		/// Can be used to improve performance if an expensive notification
		/// shall only be created and posted if there are any observers.
		
	std::size_t countObservers() const;
		/// Returns the number of registered observers.
		
	static NotificationCenter& defaultCenter();
		/// Returns a reference to the default
		/// NotificationCenter.

private:
	typedef SharedPtr<AbstractObserver> AbstractObserverPtr;
	typedef std::vector<AbstractObserverPtr> ObserverList;

	ObserverList  _observers;
	mutable Mutex _mutex;
};


} // namespace Poco


#endif // Foundation_NotificationCenter_INCLUDED
