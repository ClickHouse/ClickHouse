//
// Attr.cpp
//
// $Id: //poco/1.4/XML/src/Attr.cpp#1 $
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


#include "Poco/DOM/Attr.h"
#include "Poco/DOM/Document.h"
#include "Poco/XML/NamePool.h"


namespace Poco {
namespace XML {


Attr::Attr(Document* pOwnerDocument, Element* pOwnerElement, const XMLString& namespaceURI, const XMLString& localName, const XMLString& qname, const XMLString& value, bool specified):
	AbstractNode(pOwnerDocument),
	_name(pOwnerDocument->namePool().insert(qname, namespaceURI, localName)),
	_value(value),
	_specified(specified)
{
}


Attr::Attr(Document* pOwnerDocument, const Attr& attr): 
	AbstractNode(pOwnerDocument, attr),
	_name(pOwnerDocument->namePool().insert(attr._name)),
	_value(attr._value),
	_specified(attr._specified)
{
}


Attr::~Attr()
{
}


void Attr::setValue(const XMLString& value)
{
	XMLString oldValue = _value;
	_value     = value;
	_specified = true;
	if (_pParent && !_pOwner->eventsSuspended())
		_pParent->dispatchAttrModified(this, MutationEvent::MODIFICATION, oldValue, value);
}


Node* Attr::parentNode() const
{
	return 0;
}


Node* Attr::previousSibling() const
{
	if (_pParent)
	{
		Attr* pSibling = static_cast<Element*>(_pParent)->_pFirstAttr;
		while (pSibling)
		{
		    if (pSibling->_pNext == const_cast<Attr*>(this)) return pSibling;
		    pSibling = static_cast<Attr*>(pSibling->_pNext);
		}
		return pSibling;
	}
	return 0;
}


const XMLString& Attr::nodeName() const
{
	return _name.qname();
}


const XMLString& Attr::getNodeValue() const
{
	return _value;
}


void Attr::setNodeValue(const XMLString& value)
{
	setValue(value);
}


unsigned short Attr::nodeType() const
{
	return ATTRIBUTE_NODE;
}


const XMLString& Attr::namespaceURI() const
{
	return _name.namespaceURI();
}


XMLString Attr::prefix() const
{
	return _name.prefix();
}


const XMLString& Attr::localName() const
{
	return _name.localName();
}


XMLString Attr::innerText() const
{
	return nodeValue();
}


Node* Attr::copyNode(bool deep, Document* pOwnerDocument) const
{
	return new Attr(pOwnerDocument, *this);
}


} } // namespace Poco::XML
