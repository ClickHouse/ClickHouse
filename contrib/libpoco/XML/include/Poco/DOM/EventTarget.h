//
// EventTarget.h
//
// $Id: //poco/1.4/XML/include/Poco/DOM/EventTarget.h#1 $
//
// Library: XML
// Package: DOM
// Module:  DOMEvents
//
// Definition of the DOM EventTarget interface.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DOM_EventTarget_INCLUDED
#define DOM_EventTarget_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/DOM/DOMObject.h"
#include "Poco/XML/XMLString.h"


namespace Poco {
namespace XML {


class EventListener;
class Event;


class XML_API EventTarget: public DOMObject
	/// The EventTarget interface is implemented by all Nodes in an implementation
	/// which supports the DOM Event Model. Therefore, this interface can be obtained
	/// by using binding-specific casting methods on an instance of the Node interface.
	/// The interface allows registration and removal of EventListeners on an EventTarget
	/// and dispatch of events to that EventTarget.
{
public:
	virtual void addEventListener(const XMLString& type, EventListener* listener, bool useCapture) = 0;
		/// This method allows the registration of event listeners on 
		/// the event target. If an EventListener is added to an
		/// EventTarget while it is processing an event, it will not 
		/// be triggered by the current actions but may be triggered
		/// during a later stage of event flow, such as the bubbling phase.
		/// If multiple identical EventListeners are registered on the same 
		/// EventTarget with the same parameters the duplicate instances are 
		/// discarded. They do not cause the EventListener to be called twice and since they are
		/// discarded they do not need to be removed with the removeEventListener method. 	
	
	virtual void removeEventListener(const XMLString& type, EventListener* listener, bool useCapture) = 0;
		/// This method allows the removal of event listeners from the event 
		/// target. If an EventListener is removed from an EventTarget while it is 
		/// processing an event, it will not be triggered by the current actions. 
		/// EventListeners can never be invoked after being removed.
		/// Calling removeEventListener with arguments which do not identify 
		/// any currently registered EventListener on the EventTarget has no effect. 

	virtual bool dispatchEvent(Event* evt) = 0;
		/// This method allows the dispatch of events into the implementations 
		/// event model. Events dispatched in this manner will have the same capturing and 
		/// bubbling behavior as events dispatched directly by the
		/// implementation. The target of the event is the EventTarget on 
		/// which dispatchEvent is called. 

protected:
	virtual ~EventTarget();
};


} } // namespace Poco::XML


#endif // DOM_EventTarget_INCLUDED
