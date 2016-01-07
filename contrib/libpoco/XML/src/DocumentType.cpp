//
// DocumentType.cpp
//
// $Id: //poco/1.4/XML/src/DocumentType.cpp#1 $
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


#include "Poco/DOM/DocumentType.h"
#include "Poco/DOM/Document.h"
#include "Poco/DOM/DTDMap.h"
#include "Poco/DOM/DOMException.h"


namespace Poco {
namespace XML {


DocumentType::DocumentType(Document* pOwner, const XMLString& name, const XMLString& publicId, const XMLString& systemId): 
	AbstractContainerNode(pOwner),
	_name(name),
	_publicId(publicId),
	_systemId(systemId)
{
}


DocumentType::DocumentType(Document* pOwner, const DocumentType& doctype): 
	AbstractContainerNode(pOwner, doctype),
	_name(doctype._name),
	_publicId(doctype._publicId),
	_systemId(doctype._systemId)
{
}


DocumentType::~DocumentType()
{
}


NamedNodeMap* DocumentType::entities() const
{
	return new DTDMap(this, Node::ENTITY_NODE);
}


NamedNodeMap* DocumentType::notations() const
{
	return new DTDMap(this, Node::NOTATION_NODE);
}


const XMLString& DocumentType::nodeName() const
{
	return _name;
}


unsigned short DocumentType::nodeType() const
{
	return Node::DOCUMENT_TYPE_NODE;
}


const XMLString& DocumentType::internalSubset() const
{
	return EMPTY_STRING;
}


Node* DocumentType::copyNode(bool deep, Document* pOwnerDocument) const
{
	return new DocumentType(pOwnerDocument, *this);
}


} } // namespace Poco::XML
