//
// Entity.h
//
// $Id: //poco/1.4/XML/include/Poco/DOM/Entity.h#1 $
//
// Library: XML
// Package: DOM
// Module:  DOM
//
// Definition of the DOM Entity class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DOM_Entity_INCLUDED
#define DOM_Entity_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/DOM/AbstractContainerNode.h"
#include "Poco/XML/XMLString.h"


namespace Poco {
namespace XML {


class XML_API Entity: public AbstractContainerNode
	/// This interface represents an entity, either parsed or unparsed, in an XML
	/// document. Note that this models the entity itself not the entity declaration.
	/// Entity declaration modeling has been left for a later Level of the DOM
	/// specification.
	/// 
	/// The nodeName attribute that is inherited from Node contains the name of
	/// the entity.
	/// 
	/// An XML processor may choose to completely expand entities before the structure
	/// model is passed to the DOM; in this case there will be no EntityReference
	/// nodes in the document tree.
	/// 
	/// XML does not mandate that a non-validating XML processor read and process
	/// entity declarations made in the external subset or declared in external
	/// parameter entities. This means that parsed entities declared in the external
	/// subset need not be expanded by some classes of applications, and that the
	/// replacement value of the entity may not be available. When the replacement
	/// value is available, the corresponding Entity node's child list represents
	/// the structure of that replacement text. Otherwise, the child list is empty.
	/// 
	/// The resolution of the children of the Entity (the replacement value) may
	/// be lazily evaluated; actions by the user (such as calling the childNodes
	/// method on the Entity Node) are assumed to trigger the evaluation.
	/// 
	/// The DOM Level 1 does not support editing Entity nodes; if a user wants to
	/// make changes to the contents of an Entity, every related EntityReference
	/// node has to be replaced in the structure model by a clone of the Entity's
	/// contents, and then the desired changes must be made to each of those clones
	/// instead. Entity nodes and all their descendants are readonly.
	/// 
	/// An Entity node does not have any parent.
{
public:
	const XMLString& publicId() const;
		/// Returns the public identifier associated with
		/// the entity, if specified. If the public identifier
		/// was not specified, this is the empty string.

	const XMLString& systemId() const;
		/// Returns the system identifier associated with
		/// the entity, if specified. If the system identifier
		/// was not specified, this is the empty string.

	const XMLString& notationName() const;
		/// Returns, for unparsed entities, the name of the
		/// notation for the entity. For parsed entities, this
		/// is the empty string.

	// Node
	const XMLString& nodeName() const;
	unsigned short nodeType() const;

protected:
	Entity(Document* pOwnerDocument, const XMLString& name, const XMLString& publicId, const XMLString& systemId, const XMLString& notationName);
	Entity(Document* pOwnerDocument, const Entity& entity);
	~Entity();
	
	Node* copyNode(bool deep, Document* pOwnerDocument) const;

private:
	static const XMLString NODE_NAME;

	XMLString _name;
	XMLString _publicId;
	XMLString _systemId;
	XMLString _notationName;

	friend class Document;
};


//
// inlines
//
inline const XMLString& Entity::publicId() const
{
	return _publicId;
}


inline const XMLString& Entity::systemId() const
{
	return _systemId;
}


inline const XMLString& Entity::notationName() const
{
	return _notationName;
}


} } // namespace Poco::XML


#endif // DOM_Entity_INCLUDED
