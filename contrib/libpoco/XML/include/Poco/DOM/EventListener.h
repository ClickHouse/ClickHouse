//
// EventListener.h
//
// $Id: //poco/1.4/XML/include/Poco/DOM/EventListener.h#1 $
//
// Library: XML
// Package: DOM
// Module:  DOMEvents
//
// Definition of the DOM EventListener interface.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DOM_EventListener_INCLUDED
#define DOM_EventListener_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/XML/XMLString.h"


namespace Poco {
namespace XML {


class Event;


class XML_API EventListener
	/// The EventListener interface is the primary method for handling events. Users
	/// implement the EventListener interface and register their listener on an
	/// EventTarget using the AddEventListener method. The users should also remove
	/// their EventListener from its EventTarget after they have completed using
	/// the listener.
	/// 
	/// When a Node is copied using the cloneNode method the EventListeners attached
	/// to the source Node are not attached to the copied Node. If the user wishes
	/// the same EventListeners to be added to the newly created copy the user must
	/// add them manually.
{
public:
	virtual void handleEvent(Event* evt) = 0;
		/// This method is called whenever an event occurs of the 
		/// type for which the EventListener interface was registered.

protected:
	virtual ~EventListener();
};


} } // namespace Poco::XML


#endif // DOM_EventListener_INCLUDED
