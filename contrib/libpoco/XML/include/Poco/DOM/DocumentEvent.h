//
// DocumentEvent.h
//
// $Id: //poco/1.4/XML/include/Poco/DOM/DocumentEvent.h#1 $
//
// Library: XML
// Package: DOM
// Module:  DOM
//
// Definition of the DOM DocumentEvent interface.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DOM_DocumentEvent_INCLUDED
#define DOM_DocumentEvent_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/XML/XMLString.h"


namespace Poco {
namespace XML {


class Event;


class XML_API DocumentEvent
	/// The DocumentEvent interface provides a mechanism by which the user can create
	/// an Event of a type supported by the implementation. It is expected that
	/// the DocumentEvent interface will be implemented on the same object which
	/// implements the Document interface in an implementation which supports the
	/// Event model.
{
public:
	virtual Event* createEvent(const XMLString& eventType) const = 0;
		/// Creates an event of the specified type.
		///
		/// The eventType parameter specifies the type of Event interface to be created.
		/// If the Event interface specified is supported by the implementation this
		/// method will return a new Event of the interface type requested. If the Event
		/// is to be dispatched via the dispatchEvent method the appropriate event init
		/// method must be called after creation in order to initialize the Event's
		/// values. As an example, a user wishing to synthesize some kind of UIEvent
		/// would call createEvent with the parameter "UIEvents". The initUIEvent method
		/// could then be called on the newly created UIEvent to set the specific type
		/// of UIEvent to be dispatched and set its context information.
		/// The createEvent method is used in creating Events when it is either inconvenient
		/// or unnecessary for the user to create an Event themselves. In cases where
		/// the implementation provided Event is insufficient, users may supply their
		/// own Event implementations for use with the dispatchEvent method.

protected:
	virtual ~DocumentEvent();
};


} } // namespace Poco::XML


#endif // DOM_DocumentEvent_INCLUDED
