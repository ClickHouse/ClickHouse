//
// EventDispatcher.h
//
// Library: XML
// Package: DOM
// Module:  DOMEvents
//
// Definition of the EventDispatcher class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DOM_EventDispatcher_INCLUDED
#define DOM_EventDispatcher_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/XML/XMLString.h"
#include <list>


namespace Poco {
namespace XML {


class Event;
class EventListener;


class XML_API EventDispatcher
	/// This helper class manages event listener subscriptions
	/// and event dispatching for AbstractNode.
	///
	/// The EventListener list is managed in such a way that
	/// event listeners can be added and removed even
	/// from within an EventListener, while events are being
	/// dispatched.
{
public:
	EventDispatcher();
		/// Creates the EventDispatcher.
		
	~EventDispatcher();
		/// Destroys the EventDispatcher.
		
	void addEventListener(const XMLString& type, EventListener* listener, bool useCapture);
		/// Adds an EventListener to the internal list.
		
	void removeEventListener(const XMLString& type, EventListener* listener, bool useCapture);
		/// Removes an EventListener from the internal list.
		///
		/// If a dispatch is currently in progress, the list
		/// entry is only marked for deletion.
		/// If no dispatch is currently in progress, all EventListeners
		/// marked for deletion are removed from the list.

	void dispatchEvent(Event* evt);
		/// Dispatches the event.
		///
		/// Also removes all EventListeners marked for deletion from the 
		/// event dispatcher list.
		
	void captureEvent(Event* evt);
		/// Dispatches the event in its capturing phase.
		///
		/// Also removes all EventListeners marked for deletion from the 
		/// event dispatcher list.
		
	void bubbleEvent(Event* evt);
		/// Dispatches the event in its bubbling phase.
		///
		/// Also removes all EventListeners marked for deletion from the 
		/// event dispatcher list.

private:
	struct EventListenerItem
	{
		XMLString      type;
		EventListener* pListener;
		bool           useCapture;
	};

	typedef std::list<EventListenerItem> EventListenerList;
	
	int               _inDispatch;
	EventListenerList _listeners;
};


} } // namespace Poco::XML


#endif // DOM_EventDispatcher_INCLUDED
