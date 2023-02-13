//
// SocketReactor.h
//
// Library: Net
// Package: Reactor
// Module:  SocketReactor
//
// Definition of the SocketReactor class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_SocketReactor_INCLUDED
#define Net_SocketReactor_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/Socket.h"
#include "Poco/Runnable.h"
#include "Poco/Timespan.h"
#include "Poco/Observer.h"
#include "Poco/AutoPtr.h"
#include <map>


namespace Poco {


class Thread;


namespace Net {


class Socket;
class SocketNotification;
class SocketNotifier;


class Net_API SocketReactor: public Poco::Runnable
	/// This class, which is part of the Reactor pattern,
	/// implements the "Initiation Dispatcher".
	///
	/// The Reactor pattern has been described in the book
	/// "Pattern Languages of Program Design" by Jim Coplien
	/// and Douglas C. Schmidt (Addison Wesley, 1995).
	///
	/// The Reactor design pattern handles service requests that
	/// are delivered concurrently to an application by one or more
	/// clients. Each service in an application may consist of several
	/// methods and is represented by a separate event handler. The event
	/// handler is responsible for servicing service-specific requests.
	/// The SocketReactor dispatches the event handlers.
	///
	/// Event handlers (any class can be an event handler - there
	/// is no base class for event handlers) can be registered
	/// with the addEventHandler() method and deregistered with
	/// the removeEventHandler() method.
	/// 
	/// An event handler is always registered for a certain socket,
	/// which is given in the call to addEventHandler(). Any method
	/// of the event handler class can be registered to handle the
	/// event - the only requirement is that the method takes
	/// a pointer to an instance of SocketNotification (or a subclass of it)
	/// as argument.
	///
	/// Once started, the SocketReactor waits for events
	/// on the registered sockets, using Socket::select().
	/// If an event is detected, the corresponding event handler
	/// is invoked. There are five event types (and corresponding
	/// notification classes) defined: ReadableNotification, WritableNotification,
	/// ErrorNotification, TimeoutNotification, IdleNotification and 
	/// ShutdownNotification.
	/// 
	/// The ReadableNotification will be dispatched if a socket becomes
	/// readable. The WritableNotification will be dispatched if a socket
	/// becomes writable. The ErrorNotification will be dispatched if
	/// there is an error condition on a socket.
	///
	/// If the timeout expires and no event has occurred, a
	/// TimeoutNotification will be dispatched to all event handlers
	/// registered for it. This is done in the onTimeout() method
	/// which can be overridden by subclasses to perform custom
	/// timeout processing.
	///
	/// If there are no sockets for the SocketReactor to pass to
	/// Socket::select(), an IdleNotification will be dispatched to
	/// all event handlers registered for it. This is done in the
	/// onIdle() method which can be overridden by subclasses
	/// to perform custom idle processing. Since onIdle() will be
	/// called repeatedly in a loop, it is recommended to do a
	/// short sleep or yield in the event handler.
	///
	/// Finally, when the SocketReactor is about to shut down (as a result 
	/// of stop() being called), it dispatches a ShutdownNotification
	/// to all event handlers. This is done in the onShutdown() method
	/// which can be overridded by subclasses to perform custom
	/// shutdown processing.
	///
	/// The SocketReactor is implemented so that it can 
	/// run in its own thread. It is also possible to run
	/// multiple SocketReactors in parallel, as long as
	/// they work on different sockets.
	///
	/// It is safe to call addEventHandler() and removeEventHandler()
	/// from another thread while the SocketReactor is running. Also,
	/// it is safe to call addEventHandler() and removeEventHandler()
	/// from event handlers.
{
public:
	SocketReactor();
		/// Creates the SocketReactor.

	explicit SocketReactor(const Poco::Timespan& timeout);
		/// Creates the SocketReactor, using the given timeout.

	virtual ~SocketReactor();
		/// Destroys the SocketReactor.

	void run();
		/// Runs the SocketReactor. The reactor will run
		/// until stop() is called (in a separate thread).
		
	void stop();
		/// Stops the SocketReactor.
		///
		/// The reactor will be stopped when the next event
		/// (including a timeout event) occurs.

	void wakeUp();
		/// Wakes up idle reactor.

	void setTimeout(const Poco::Timespan& timeout);
		/// Sets the timeout. 
		///
		/// If no other event occurs for the given timeout 
		/// interval, a timeout event is sent to all event listeners.
		///
		/// The default timeout is 250 milliseconds;
		///
		/// The timeout is passed to the Socket::select()
		/// method.
		
	const Poco::Timespan& getTimeout() const;
		/// Returns the timeout.

	void addEventHandler(const Socket& socket, const Poco::AbstractObserver& observer);
		/// Registers an event handler with the SocketReactor.
		///
		/// Usage:
		///     Poco::Observer<MyEventHandler, SocketNotification> obs(*this, &MyEventHandler::handleMyEvent);
		///     reactor.addEventHandler(obs);

	bool hasEventHandler(const Socket& socket, const Poco::AbstractObserver& observer);
		/// Returns true if the observer is registered with SocketReactor for the given socket.

	void removeEventHandler(const Socket& socket, const Poco::AbstractObserver& observer);
		/// Unregisters an event handler with the SocketReactor.
		///
		/// Usage:
		///     Poco::Observer<MyEventHandler, SocketNotification> obs(*this, &MyEventHandler::handleMyEvent);
		///     reactor.removeEventHandler(obs);

protected:
	virtual void onTimeout();
		/// Called if the timeout expires and no other events are available.
		///
		/// Can be overridden by subclasses. The default implementation
		/// dispatches the TimeoutNotification and thus should be called by overriding
		/// implementations.

	virtual void onIdle();
		/// Called if no sockets are available to call select() on.
		///
		/// Can be overridden by subclasses. The default implementation
		/// dispatches the IdleNotification and thus should be called by overriding
		/// implementations.

	virtual void onShutdown();
		/// Called when the SocketReactor is about to terminate.
		///
		/// Can be overridden by subclasses. The default implementation
		/// dispatches the ShutdownNotification and thus should be called by overriding
		/// implementations.

	virtual void onBusy();
		/// Called when the SocketReactor is busy and at least one notification
		/// has been dispatched.
		///
		/// Can be overridden by subclasses to perform additional
		/// periodic tasks. The default implementation does nothing.

	void dispatch(const Socket& socket, SocketNotification* pNotification);
		/// Dispatches the given notification to all observers
		/// registered for the given socket.
		
	void dispatch(SocketNotification* pNotification);
		/// Dispatches the given notification to all observers.

private:
	typedef Poco::AutoPtr<SocketNotifier>     NotifierPtr;
	typedef Poco::AutoPtr<SocketNotification> NotificationPtr;
	typedef std::map<Socket, NotifierPtr>     EventHandlerMap;

	void dispatch(NotifierPtr& pNotifier, SocketNotification* pNotification);

	enum
	{
		DEFAULT_TIMEOUT = 250000
	};

	bool            _stop;
	Poco::Timespan  _timeout;
	EventHandlerMap _handlers;
	NotificationPtr _pReadableNotification;
	NotificationPtr _pWritableNotification;
	NotificationPtr _pErrorNotification;
	NotificationPtr _pTimeoutNotification;
	NotificationPtr _pIdleNotification;
	NotificationPtr _pShutdownNotification;
	Poco::FastMutex _mutex;
	Poco::Thread*   _pThread;
	
	friend class SocketNotifier;
};


} } // namespace Poco::Net


#endif // Net_SocketReactor_INCLUDED
