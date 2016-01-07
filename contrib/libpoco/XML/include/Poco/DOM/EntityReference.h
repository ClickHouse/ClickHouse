//
// EntityReference.h
//
// $Id: //poco/1.4/XML/include/Poco/DOM/EntityReference.h#1 $
//
// Library: XML
// Package: DOM
// Module:  DOM
//
// Definition of the DOM EntityReference class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DOM_EntityReference_INCLUDED
#define DOM_EntityReference_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/DOM/AbstractNode.h"
#include "Poco/XML/XMLString.h"


namespace Poco {
namespace XML {


class XML_API EntityReference: public AbstractNode
	/// EntityReference objects may be inserted into the structure model when an
	/// entity reference is in the source document, or when the user wishes to insert
	/// an entity reference. Note that character references and references to predefined
	/// entities are considered to be expanded by the HTML or XML processor so that
	/// characters are represented by their Unicode equivalent rather than by an
	/// entity reference. Moreover, the XML processor may completely expand references
	/// to entities while building the structure model, instead of providing EntityReference
	/// objects. If it does provide such objects, then for a given EntityReference
	/// node, it may be that there is no Entity node representing the referenced
	/// entity. If such an Entity exists, then the child list of the EntityReference
	/// node is the same as that of the Entity node.
	/// 
	/// As for Entity nodes, EntityReference nodes and all their descendants are
	/// readonly.
	/// 
	/// The resolution of the children of the EntityReference (the replacement value
	/// of the referenced Entity) may be lazily evaluated; actions by the user (such
	/// as calling the childNodes method on the EntityReference node) are assumed
	/// to trigger the evaluation.
{
public:
	// Node
	const XMLString& nodeName() const;
	unsigned short nodeType() const;

protected:
	EntityReference(Document* pOwnerDocument, const XMLString& name);
	EntityReference(Document* pOwnerDocument, const EntityReference& ref);
	~EntityReference();

	Node* copyNode(bool deep, Document* pOwnerDocument) const;

private:
	XMLString _name;
	
	friend class Document;
};


} } // namespace Poco::XML


#endif // DOM_EntityReference_INCLUDED
