//
// Notation.cpp
//
// $Id: //poco/1.4/XML/src/Notation.cpp#1 $
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


#include "Poco/DOM/Notation.h"


namespace Poco {
namespace XML {


Notation::Notation(Document* pOwnerDocument, const XMLString& name, const XMLString& publicId, const XMLString& systemId): 
	AbstractNode(pOwnerDocument),
	_name(name),
	_publicId(publicId),
	_systemId(systemId)
{
}


Notation::Notation(Document* pOwnerDocument, const Notation& notation): 
	AbstractNode(pOwnerDocument, notation),
	_name(notation._name),
	_publicId(notation._publicId),
	_systemId(notation._systemId)
{
}


Notation::~Notation()
{
}


const XMLString& Notation::nodeName() const
{
	return _name;
}


unsigned short Notation::nodeType() const
{
	return Node::NOTATION_NODE;
}


Node* Notation::copyNode(bool deep, Document* pOwnerDocument) const
{
	return new Notation(pOwnerDocument, *this);
}


} } // namespace Poco::XML
