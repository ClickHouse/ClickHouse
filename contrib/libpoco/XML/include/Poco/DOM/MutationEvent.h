//
// MutationEvent.h
//
// $Id: //poco/1.4/XML/include/Poco/DOM/MutationEvent.h#1 $
//
// Library: XML
// Package: DOM
// Module:  DOMEvents
//
// Definition of the DOM MutationEvent class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DOM_MutationEvent_INCLUDED
#define DOM_MutationEvent_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/DOM/Event.h"


namespace Poco {
namespace XML {


class Node;


class XML_API MutationEvent: public Event
	/// The MutationEvent interface provides specific contextual 
	/// information associated with Mutation events.
{
public:
	enum AttrChangeType
	{
		MODIFICATION = 1, /// The Attr was modified in place.
		ADDITION     = 2, /// The Attr was just added. 
		REMOVAL      = 3  /// The Attr was just removed. 
	};

	Node* relatedNode() const;
		/// relatedNode is used to identify a secondary node related to a mutation 
		/// event. For example, if a mutation event is dispatched
		/// to a node indicating that its parent has changed, the relatedNode is the 
		/// changed parent. If an event is instead dispatched to a
		/// subtree indicating a node was changed within it, the relatedNode is 
		/// the changed node. In the case of the DOMAttrModified
		/// event it indicates the Attr node which was modified, added, or removed.

	const XMLString& prevValue() const;
		/// prevValue indicates the previous value of the Attr node in DOMAttrModified 
		/// events, and of the CharacterData node in DOMCharDataModified events.

	const XMLString& newValue() const;
		/// newValue indicates the new value of the Attr node in DOMAttrModified 
		/// events, and of the CharacterData node in DOMCharDataModified events.

	const XMLString& attrName() const;
		/// attrName indicates the name of the changed Attr node in a DOMAttrModified event.

	AttrChangeType attrChange() const;
		/// attrChange indicates the type of change which triggered the 
		/// DOMAttrModified event. The values can be MODIFICATION,
		/// ADDITION, or REMOVAL.

	void initMutationEvent(const XMLString& type, bool canBubble, bool cancelable, Node* relatedNode, 
	                       const XMLString& prevValue, const XMLString& newValue, const XMLString& attrName, AttrChangeType change);
		/// The initMutationEvent method is used to initialize the value of a 
		/// MutationEvent created through the DocumentEvent
		/// interface. This method may only be called before the MutationEvent 
		/// has been dispatched via the dispatchEvent method,
		/// though it may be called multiple times during that phase if 
		/// necessary. If called multiple times, the final invocation takes
		/// precedence.

	// Event Types
	static const XMLString DOMSubtreeModified;
	static const XMLString DOMNodeInserted;
	static const XMLString DOMNodeRemoved;
	static const XMLString DOMNodeRemovedFromDocument;
	static const XMLString DOMNodeInsertedIntoDocument;
	static const XMLString DOMAttrModified;
	static const XMLString DOMCharacterDataModified;

protected:
	MutationEvent(Document* pOwnerDocument, const XMLString& type);
	MutationEvent(Document* pOwnerDocument, const XMLString& type, EventTarget* pTarget, bool canBubble, bool cancelable, Node* relatedNode);
	MutationEvent(Document* pOwnerDocument, const XMLString& type, EventTarget* pTarget, bool canBubble, bool cancelable, Node* relatedNode, 
				  const XMLString& prevValue, const XMLString& newValue, const XMLString& attrName, AttrChangeType change);
	~MutationEvent();

private:
	XMLString      _prevValue;
	XMLString      _newValue;
	XMLString      _attrName;
	AttrChangeType _change;
	Node*          _pRelatedNode;

	friend class AbstractNode;
	friend class Document;
};


//
// inlines
//
inline Node* MutationEvent::relatedNode() const
{
	return _pRelatedNode;
}


inline const XMLString& MutationEvent::prevValue() const
{
	return _prevValue;
}


inline const XMLString& MutationEvent::newValue() const
{
	return _newValue;
}


inline const XMLString& MutationEvent::attrName() const
{
	return _attrName;
}


inline MutationEvent::AttrChangeType MutationEvent::attrChange() const
{
	return _change;
}


} } // namespace Poco::XML


#endif // DOM_MutationEvent_INCLUDED
