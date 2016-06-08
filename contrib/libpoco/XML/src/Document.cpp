//
// Document.cpp
//
// $Id: //poco/1.4/XML/src/Document.cpp#2 $
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


#include "Poco/DOM/Document.h"
#include "Poco/DOM/DocumentType.h"
#include "Poco/DOM/DOMImplementation.h"
#include "Poco/DOM/Element.h"
#include "Poco/DOM/Attr.h"
#include "Poco/DOM/DocumentFragment.h"
#include "Poco/DOM/Text.h"
#include "Poco/DOM/Comment.h"
#include "Poco/DOM/CDATASection.h"
#include "Poco/DOM/ProcessingInstruction.h"
#include "Poco/DOM/EntityReference.h"
#include "Poco/DOM/DOMException.h"
#include "Poco/DOM/ElementsByTagNameList.h"
#include "Poco/DOM/Entity.h"
#include "Poco/DOM/Notation.h"
#include "Poco/XML/Name.h"
#include "Poco/XML/NamePool.h"


namespace Poco {
namespace XML {


const XMLString Document::NODE_NAME = toXMLString("#document");


Document::Document(NamePool* pNamePool): 
	AbstractContainerNode(0),
	_pDocumentType(0),
	_eventSuspendLevel(0)
{
	if (pNamePool)
	{
		_pNamePool = pNamePool;
		_pNamePool->duplicate();
	}
	else
	{
		_pNamePool = new NamePool;
	}
}


Document::Document(DocumentType* pDocumentType, NamePool* pNamePool): 
	AbstractContainerNode(0),
	_pDocumentType(pDocumentType),
	_eventSuspendLevel(0)
{
	if (pNamePool)
	{
		_pNamePool = pNamePool;
		_pNamePool->duplicate();
	}
	else
	{
		_pNamePool = new NamePool;
	}
	if (_pDocumentType)
	{
		_pDocumentType->duplicate();
		_pDocumentType->setOwnerDocument(this);
	}
}


Document::~Document()
{
	if (_pDocumentType) _pDocumentType->release();
	_pNamePool->release();
}


bool Document::dispatchEvent(Event* evt)
{
	return _eventSuspendLevel > 0 || AbstractContainerNode::dispatchEvent(evt);
}


void Document::collectGarbage()
{
	_autoReleasePool.release();
}


void Document::suspendEvents()
{
	++_eventSuspendLevel;
}


void Document::resumeEvents()
{
	poco_assert_dbg (_eventSuspendLevel > 0);

	--_eventSuspendLevel;
}


const DOMImplementation& Document::implementation() const
{
	return DOMImplementation::instance();
}


Element* Document::documentElement() const
{
	// Skip non-element nodes before the document element
	Node* pCur = firstChild();
	while (pCur)
	{
		if (dynamic_cast<Element*>(pCur))
			return static_cast<Element*>(pCur);
		pCur = pCur->nextSibling();
	}
	return 0;
}


Element* Document::createElement(const XMLString& tagName) const
{
	return new Element(const_cast<Document*>(this), EMPTY_STRING, EMPTY_STRING, tagName); 
}


DocumentFragment* Document::createDocumentFragment() const
{
	return new DocumentFragment(const_cast<Document*>(this));
}


Text* Document::createTextNode(const XMLString& data) const
{
	return new Text(const_cast<Document*>(this), data);
}


Comment* Document::createComment(const XMLString& data) const
{
	return new Comment(const_cast<Document*>(this), data);
}


CDATASection* Document::createCDATASection(const XMLString& data) const
{
	return new CDATASection(const_cast<Document*>(this), data);
}


ProcessingInstruction* Document::createProcessingInstruction(const XMLString& target, const XMLString& data) const
{
	return new ProcessingInstruction(const_cast<Document*>(this), target, data);
}


Attr* Document::createAttribute(const XMLString& name) const
{
	return new Attr(const_cast<Document*>(this), 0, EMPTY_STRING, EMPTY_STRING, name, EMPTY_STRING);
}


EntityReference* Document::createEntityReference(const XMLString& name) const
{
	return new EntityReference(const_cast<Document*>(this), name);
}


NodeList* Document::getElementsByTagName(const XMLString& name) const
{
	return new ElementsByTagNameList(const_cast<Document*>(this), name);	
}


const XMLString& Document::nodeName() const
{
	return NODE_NAME;
}


unsigned short Document::nodeType() const
{
	return Node::DOCUMENT_NODE;
}


Node* Document::importNode(Node* importedNode, bool deep)
{
	return static_cast<AbstractNode*>(importedNode)->copyNode(deep, this);
}


Element* Document::createElementNS(const XMLString& namespaceURI, const XMLString& qualifiedName) const
{
	return new Element(const_cast<Document*>(this), namespaceURI, Name::localName(qualifiedName), qualifiedName);
}


Attr* Document::createAttributeNS(const XMLString& namespaceURI, const XMLString& qualifiedName) const
{
	return new Attr(const_cast<Document*>(this), 0, namespaceURI, Name::localName(qualifiedName), qualifiedName, EMPTY_STRING);
}


NodeList* Document::getElementsByTagNameNS(const XMLString& namespaceURI, const XMLString& localName) const
{
	return new ElementsByTagNameListNS(const_cast<Document*>(this), namespaceURI, localName);	
}


Element* Document::getElementById(const XMLString& elementId) const
{
	return 0;
}


Event* Document::createEvent(const XMLString& eventType) const
{
	if (eventType == MutationEvent::DOMSubtreeModified          ||
	    eventType == MutationEvent::DOMNodeInserted             ||
		eventType == MutationEvent::DOMNodeRemoved              ||
		eventType == MutationEvent::DOMNodeRemovedFromDocument  ||
		eventType == MutationEvent::DOMNodeInsertedIntoDocument ||
		eventType == MutationEvent::DOMAttrModified             ||
		eventType == MutationEvent::DOMCharacterDataModified)
	{
		return new MutationEvent(const_cast<Document*>(this), eventType);
	}
	throw DOMException(DOMException::NOT_SUPPORTED_ERR);
}


Node* Document::copyNode(bool deep, Document* pOwnerDocument) const
{
	throw DOMException(DOMException::NOT_SUPPORTED_ERR);
}


void Document::setDoctype(DocumentType* pDoctype)
{
	if (_pDocumentType) _pDocumentType->release();
	_pDocumentType = pDoctype;
	if (_pDocumentType)
	{
		_pDocumentType->duplicate();
		_pDocumentType->setOwnerDocument(this);
	}
}


bool Document::eventsSuspended() const
{
	return _eventSuspendLevel > 0;
}


bool Document::events() const
{
	return _eventSuspendLevel == 0;
}


Entity* Document::createEntity(const XMLString& name, const XMLString& publicId, const XMLString& systemId, const XMLString& notationName) const
{
	return new Entity(const_cast<Document*>(this), name, publicId, systemId, notationName);
}


Notation* Document::createNotation(const XMLString& name, const XMLString& publicId, const XMLString& systemId) const
{
	return new Notation(const_cast<Document*>(this), name, publicId, systemId);
}


Element* Document::getElementById(const XMLString& elementId, const XMLString& idAttribute) const
{
	Element* pElem = documentElement();
	if (pElem) pElem = pElem->getElementById(elementId, idAttribute);
	return pElem;
}


Element* Document::getElementByIdNS(const XMLString& elementId, const XMLString& idAttributeURI, const XMLString& idAttributeLocalName) const
{
	Element* pElem = documentElement();
	if (pElem) pElem = pElem->getElementByIdNS(elementId, idAttributeURI, idAttributeLocalName);
	return pElem;
}


} } // namespace Poco::XML
