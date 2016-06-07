//
// EntityReference.cpp
//
// $Id: //poco/1.4/XML/src/EntityReference.cpp#1 $
//
// Library: XML
// Package: DOM
// Module:  DOM
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/DOM/EntityReference.h"


namespace Poco {
namespace XML {


EntityReference::EntityReference(Document* pOwnerDocument, const XMLString& name): 
	AbstractNode(pOwnerDocument),
	_name(name)
{
}


EntityReference::EntityReference(Document* pOwnerDocument, const EntityReference& ref): 
	AbstractNode(pOwnerDocument, ref),
	_name(ref._name)
{
}


EntityReference::~EntityReference()
{
}


const XMLString& EntityReference::nodeName() const
{
	return _name;
}


unsigned short EntityReference::nodeType() const
{
	return Node::ENTITY_REFERENCE_NODE;
}


Node* EntityReference::copyNode(bool deep, Document* pOwnerDocument) const
{
	return new EntityReference(pOwnerDocument, *this);
}


} } // namespace Poco::XML
