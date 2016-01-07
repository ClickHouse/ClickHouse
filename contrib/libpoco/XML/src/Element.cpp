//
// Element.cpp
//
// $Id: //poco/1.4/XML/src/Element.cpp#2 $
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


#include "Poco/DOM/Element.h"
#include "Poco/DOM/Document.h"
#include "Poco/DOM/Attr.h"
#include "Poco/DOM/DOMException.h"
#include "Poco/DOM/ElementsByTagNameList.h"
#include "Poco/DOM/Text.h"
#include "Poco/DOM/AttrMap.h"


namespace Poco {
namespace XML {


Element::Element(Document* pOwnerDocument, const XMLString& namespaceURI, const XMLString& localName, const XMLString& qname):
	AbstractContainerNode(pOwnerDocument),
	_name(pOwnerDocument->namePool().insert(qname, namespaceURI, localName)),
	_pFirstAttr(0)
{
}


Element::Element(Document* pOwnerDocument, const Element& element): 
	AbstractContainerNode(pOwnerDocument, element),
	_name(pOwnerDocument->namePool().insert(element._name)),
	_pFirstAttr(0)
{
	Attr* pAttr = element._pFirstAttr;
	while (pAttr)
	{
		Attr* pClonedAttr = static_cast<Attr*>(pAttr->copyNode(false, pOwnerDocument));
		setAttributeNode(pClonedAttr);
		pClonedAttr->release();
		pAttr = static_cast<Attr*>(pAttr->_pNext);
	}
}


Element::~Element()
{
	if (_pFirstAttr) _pFirstAttr->release();
}


const XMLString& Element::getAttribute(const XMLString& name) const
{
	Attr* pAttr = getAttributeNode(name);
	if (pAttr)
		return pAttr->getValue();
	else
		return EMPTY_STRING;
}


void Element::setAttribute(const XMLString& name, const XMLString& value)
{
	Attr* pAttr = getAttributeNode(name);
	if (pAttr)
	{
		pAttr->setValue(value);
	}
	else
	{
		pAttr = ownerDocument()->createAttribute(name);
		pAttr->setValue(value);
		setAttributeNode(pAttr);
		pAttr->release();
	}
}


void Element::removeAttribute(const XMLString& name)
{
	Attr* pAttr = getAttributeNode(name);
	if (pAttr) removeAttributeNode(pAttr);
}


Attr* Element::getAttributeNode(const XMLString& name) const
{
	Attr* pAttr = _pFirstAttr;
	while (pAttr && pAttr->_name.qname() != name) pAttr = static_cast<Attr*>(pAttr->_pNext);
	return pAttr;
}


Attr* Element::setAttributeNode(Attr* newAttr)
{
	poco_check_ptr (newAttr);

	if (newAttr->ownerDocument() != ownerDocument())
		throw DOMException(DOMException::WRONG_DOCUMENT_ERR);
	if (newAttr->ownerElement())
		throw DOMException(DOMException::INUSE_ATTRIBUTE_ERR);

	Attr* oldAttr = getAttributeNode(newAttr->name());
	if (oldAttr) removeAttributeNode(oldAttr);

	Attr* pCur = _pFirstAttr;
	if (pCur)
	{
		while (pCur->_pNext) pCur = static_cast<Attr*>(pCur->_pNext);
		pCur->_pNext = newAttr;
	}
	else _pFirstAttr = newAttr;
	newAttr->duplicate();
	newAttr->_pParent = this;
	if (_pOwner->events())
		dispatchAttrModified(newAttr, MutationEvent::ADDITION, EMPTY_STRING, newAttr->getValue());

	return oldAttr;
}


Attr* Element::removeAttributeNode(Attr* oldAttr)
{
	poco_check_ptr (oldAttr);

	if (_pOwner->events()) 
		dispatchAttrModified(oldAttr, MutationEvent::REMOVAL, oldAttr->getValue(), EMPTY_STRING);

	if (oldAttr != _pFirstAttr)
	{
		Attr* pCur = _pFirstAttr;
		while (pCur->_pNext != oldAttr) pCur = static_cast<Attr*>(pCur->_pNext);
		if (pCur)
		{
			pCur->_pNext = static_cast<Attr*>(pCur->_pNext->_pNext);
		}
		else throw DOMException(DOMException::NOT_FOUND_ERR);
	}
	else _pFirstAttr = static_cast<Attr*>(_pFirstAttr->_pNext);
	oldAttr->_pNext   = 0;
	oldAttr->_pParent = 0;
	oldAttr->autoRelease();

	return oldAttr;
}


Attr* Element::addAttributeNodeNP(Attr* oldAttr, Attr* newAttr)
{
	newAttr->_pParent = this;
	if (oldAttr)
	{
		oldAttr->_pNext = newAttr;
	}
	else if (_pFirstAttr)
	{
		newAttr->_pNext = _pFirstAttr;
		_pFirstAttr = newAttr;
	}
	else
	{
		_pFirstAttr = newAttr;
	}
	newAttr->duplicate();
	return newAttr;
}


NodeList* Element::getElementsByTagName(const XMLString& name) const
{
	return new ElementsByTagNameList(this, name);
}


NodeList* Element::getElementsByTagNameNS(const XMLString& namespaceURI, const XMLString& localName) const
{
	return new ElementsByTagNameListNS(this, namespaceURI, localName);
}


void Element::normalize()
{
	Node* pCur = firstChild();
	while (pCur)
	{
		if (pCur->nodeType() == Node::ELEMENT_NODE)
		{
			pCur->normalize();
		}
		else if (pCur->nodeType() == Node::TEXT_NODE)
		{
			Node* pNext = pCur->nextSibling();
			while (pNext && pNext->nodeType() == Node::TEXT_NODE)
			{
				static_cast<Text*>(pCur)->appendData(pNext->nodeValue());
				removeChild(pNext);
				pNext = pCur->nextSibling();
			}
		}
		pCur = pCur->nextSibling();
	}
}


const XMLString& Element::nodeName() const
{
	return tagName();
}


NamedNodeMap* Element::attributes() const
{
	return new AttrMap(const_cast<Element*>(this));
}


unsigned short Element::nodeType() const
{
	return Node::ELEMENT_NODE;
}


const XMLString& Element::getAttributeNS(const XMLString& namespaceURI, const XMLString& localName) const
{
	Attr* pAttr = getAttributeNodeNS(namespaceURI, localName);
	if (pAttr)
		return pAttr->getValue();
	else
		return EMPTY_STRING;
}


void Element::setAttributeNS(const XMLString& namespaceURI, const XMLString& qualifiedName, const XMLString& value)
{
	Attr* pAttr = getAttributeNodeNS(namespaceURI, qualifiedName);
	if (pAttr)
	{
		pAttr->setValue(value);
	}
	else
	{
		pAttr = _pOwner->createAttributeNS(namespaceURI, qualifiedName);
		pAttr->setValue(value);
		setAttributeNodeNS(pAttr);
		pAttr->release();
	}
}


void Element::removeAttributeNS(const XMLString& namespaceURI, const XMLString& localName)
{
	Attr* pAttr = getAttributeNodeNS(namespaceURI, localName);
	if (pAttr) removeAttributeNode(pAttr);
}


Attr* Element::getAttributeNodeNS(const XMLString& namespaceURI, const XMLString& localName) const
{
	Attr* pAttr = _pFirstAttr;
	while (pAttr && (pAttr->_name.namespaceURI() != namespaceURI || pAttr->_name.localName() != localName)) pAttr = static_cast<Attr*>(pAttr->_pNext);
	return pAttr;
}


Attr* Element::setAttributeNodeNS(Attr* newAttr)
{
	poco_check_ptr (newAttr);

	if (newAttr->ownerDocument() != ownerDocument())
		throw DOMException(DOMException::WRONG_DOCUMENT_ERR);
	if (newAttr->ownerElement())
		throw DOMException(DOMException::INUSE_ATTRIBUTE_ERR);

	Attr* oldAttr = getAttributeNodeNS(newAttr->namespaceURI(), newAttr->localName());
	if (oldAttr) removeAttributeNode(oldAttr);

	Attr* pCur = _pFirstAttr;
	if (pCur)
	{
		while (pCur->_pNext) pCur = static_cast<Attr*>(pCur->_pNext);
		pCur->_pNext = newAttr;
	}
	else _pFirstAttr = newAttr;
	newAttr->_pParent = this;
	newAttr->duplicate();
	if (_pOwner->events())
		dispatchAttrModified(newAttr, MutationEvent::ADDITION, EMPTY_STRING, newAttr->getValue());

	return oldAttr;
}


bool Element::hasAttribute(const XMLString& name) const
{
	return getAttributeNode(name) != 0;
}


bool Element::hasAttributeNS(const XMLString& namespaceURI, const XMLString& localName) const
{
	return getAttributeNodeNS(namespaceURI, localName) != 0;
}


const XMLString& Element::namespaceURI() const
{
	return _name.namespaceURI();
}


XMLString Element::prefix() const
{
	return _name.prefix();
}


const XMLString& Element::localName() const
{
	return _name.localName();
}


bool Element::hasAttributes() const
{
	return _pFirstAttr != 0;
}


XMLString Element::innerText() const
{
	XMLString result;
	Node* pChild = firstChild();
	while (pChild)
	{
		result.append(pChild->innerText());
		pChild = pChild->nextSibling();
	}
	return result;
}


Element* Element::getChildElement(const XMLString& name) const
{
	Node* pNode = firstChild();
	while (pNode && !(pNode->nodeType() == Node::ELEMENT_NODE && pNode->nodeName() == name))
		pNode = pNode->nextSibling();
	return static_cast<Element*>(pNode);
}


Element* Element::getChildElementNS(const XMLString& namespaceURI, const XMLString& localName) const
{
	Node* pNode = firstChild();
	while (pNode && !(pNode->nodeType() == Node::ELEMENT_NODE && pNode->namespaceURI() == namespaceURI && pNode->localName() == localName))
		pNode = pNode->nextSibling();
	return static_cast<Element*>(pNode);
}


void Element::dispatchNodeRemovedFromDocument()
{
	AbstractContainerNode::dispatchNodeRemovedFromDocument();
	Attr* pAttr = _pFirstAttr;
	while (pAttr)
	{
		pAttr->dispatchNodeRemovedFromDocument();
		pAttr = static_cast<Attr*>(pAttr->_pNext);
	}
}


void Element::dispatchNodeInsertedIntoDocument()
{
	AbstractContainerNode::dispatchNodeInsertedIntoDocument();
	Attr* pAttr = _pFirstAttr;
	while (pAttr)
	{
		pAttr->dispatchNodeInsertedIntoDocument();
		pAttr = static_cast<Attr*>(pAttr->_pNext);
	}
}


Node* Element::copyNode(bool deep, Document* pOwnerDocument) const
{
	Element* pClone = new Element(pOwnerDocument, *this);
	if (deep)
	{
		Node* pNode = firstChild();
		while (pNode)
		{
			pClone->appendChild(static_cast<AbstractNode*>(pNode)->copyNode(true, pOwnerDocument))->release();
			pNode = pNode->nextSibling();
		}
	}
	return pClone;
}


Element* Element::getElementById(const XMLString& elementId, const XMLString& idAttribute) const
{
	if (getAttribute(idAttribute) == elementId)
		return const_cast<Element*>(this);

	Node* pNode = firstChild();
	while (pNode)
	{
		if (pNode->nodeType() == Node::ELEMENT_NODE)
		{
			Element* pResult = static_cast<Element*>(pNode)->getElementById(elementId, idAttribute);
			if (pResult) return pResult;
		}
		pNode = pNode->nextSibling();
	}
	return 0;
}


Element* Element::getElementByIdNS(const XMLString& elementId, const XMLString& idAttributeURI, const XMLString& idAttributeLocalName) const
{
	if (getAttributeNS(idAttributeURI, idAttributeLocalName) == elementId)
		return const_cast<Element*>(this);

	Node* pNode = firstChild();
	while (pNode)
	{
		if (pNode->nodeType() == Node::ELEMENT_NODE)
		{
			Element* pResult = static_cast<Element*>(pNode)->getElementByIdNS(elementId, idAttributeURI, idAttributeLocalName);
			if (pResult) return pResult;
		}
		pNode = pNode->nextSibling();
	}
	return 0;
}


} } // namespace Poco::XML
