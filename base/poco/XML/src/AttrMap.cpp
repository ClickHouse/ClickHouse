//
// AttrMap.cpp
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


#include "Poco/DOM/AttrMap.h"
#include "Poco/DOM/Attr.h"
#include "Poco/DOM/Element.h"
#include "Poco/DOM/Document.h"
#include "Poco/DOM/DOMException.h"


namespace Poco {
namespace XML {


AttrMap::AttrMap(Element* pElement):
	_pElement(pElement)
{
	poco_check_ptr (pElement);
	
	_pElement->duplicate();
}


AttrMap::~AttrMap()
{
	_pElement->release();
}


Node* AttrMap::getNamedItem(const XMLString& name) const
{
	return _pElement->getAttributeNode(name);
}


Node* AttrMap::setNamedItem(Node* arg)
{
	poco_check_ptr (arg);

	if (arg->nodeType() != Node::ATTRIBUTE_NODE)
		throw DOMException(DOMException::HIERARCHY_REQUEST_ERR);
		
	return _pElement->setAttributeNode(static_cast<Attr*>(arg));
}


Node* AttrMap::removeNamedItem(const XMLString& name)
{
	Attr* pAttr = _pElement->getAttributeNode(name);
	if (pAttr)
		return _pElement->removeAttributeNode(pAttr);
	else
		return 0;
}


Node* AttrMap::item(unsigned long index) const
{
	AbstractNode* pAttr = _pElement->_pFirstAttr;
	while (index-- > 0 && pAttr) pAttr = static_cast<AbstractNode*>(pAttr->nextSibling());
	return pAttr;
}


unsigned long AttrMap::length() const
{
	unsigned long result = 0;
	AbstractNode* pAttr = _pElement->_pFirstAttr;
	while (pAttr) 
	{
		pAttr = static_cast<AbstractNode*>(pAttr->nextSibling());
		++result;
	}
	return result;
}


Node* AttrMap::getNamedItemNS(const XMLString& namespaceURI, const XMLString& localName) const
{
	return _pElement->getAttributeNodeNS(namespaceURI, localName);
}


Node* AttrMap::setNamedItemNS(Node* arg)
{
	poco_check_ptr (arg);

	if (arg->nodeType() != Node::ATTRIBUTE_NODE)
		throw DOMException(DOMException::HIERARCHY_REQUEST_ERR);

	return _pElement->setAttributeNodeNS(static_cast<Attr*>(arg));
}


Node* AttrMap::removeNamedItemNS(const XMLString& namespaceURI, const XMLString& localName)
{
	Attr* pAttr = _pElement->getAttributeNodeNS(namespaceURI, localName);
	if (pAttr)
		return _pElement->removeAttributeNode(pAttr);
	else
		return 0;
}


void AttrMap::autoRelease()
{
	_pElement->ownerDocument()->autoReleasePool().add(this);
}


} } // namespace Poco::XML

