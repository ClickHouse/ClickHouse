//
// DOMBuilder.cpp
//
// $Id: //poco/1.4/XML/src/DOMBuilder.cpp#1 $
//
// Library: XML
// Package: DOM
// Module:  DOMBuilder
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/DOM/DOMBuilder.h"
#include "Poco/DOM/Document.h"
#include "Poco/DOM/DocumentType.h"
#include "Poco/DOM/CharacterData.h"
#include "Poco/DOM/Text.h"
#include "Poco/DOM/Comment.h"
#include "Poco/DOM/CDATASection.h"
#include "Poco/DOM/Element.h"
#include "Poco/DOM/Attr.h"
#include "Poco/DOM/Entity.h"
#include "Poco/DOM/EntityReference.h"
#include "Poco/DOM/Notation.h"
#include "Poco/DOM/ProcessingInstruction.h"
#include "Poco/DOM/AutoPtr.h"
#include "Poco/SAX/XMLReader.h"
#include "Poco/SAX/AttributesImpl.h"


namespace Poco {
namespace XML {


const XMLString DOMBuilder::EMPTY_STRING;


DOMBuilder::DOMBuilder(XMLReader& xmlReader, NamePool* pNamePool):
	_xmlReader(xmlReader),
	_pNamePool(pNamePool),
	_pDocument(0),
	_pParent(0),
	_pPrevious(0),
	_inCDATA(false),
	_namespaces(true)
{
	_xmlReader.setContentHandler(this);
	_xmlReader.setDTDHandler(this);
	_xmlReader.setProperty(XMLReader::PROPERTY_LEXICAL_HANDLER, static_cast<LexicalHandler*>(this));

	if (_pNamePool) _pNamePool->duplicate();
}


DOMBuilder::~DOMBuilder()
{
	if (_pNamePool) _pNamePool->release();
}


Document* DOMBuilder::parse(const XMLString& uri)
{
	setupParse();
	_pDocument->suspendEvents();
	try
	{
		_xmlReader.parse(uri);
	}
	catch (...)
	{
		_pDocument->release();
		_pDocument = 0;
		_pParent   = 0;
		_pPrevious = 0;
		throw;
	}
	_pDocument->resumeEvents();
	_pDocument->collectGarbage();
	return _pDocument;
}


Document* DOMBuilder::parse(InputSource* pInputSource)
{
	setupParse();
	_pDocument->suspendEvents();
	try
	{
		_xmlReader.parse(pInputSource);
	}
	catch (...)
	{
		_pDocument->release();
		_pDocument = 0;
		_pParent   = 0;
		_pPrevious = 0;
		throw;
	}
	_pDocument->resumeEvents();
	_pDocument->collectGarbage();
	return _pDocument;
}


Document* DOMBuilder::parseMemoryNP(const char* xml, std::size_t size)
{
	setupParse();
	_pDocument->suspendEvents();
	try
	{
		_xmlReader.parseMemoryNP(xml, size);
	}
	catch (...)
	{
		_pDocument->release();
		_pDocument = 0;
		_pParent   = 0;
		_pPrevious = 0;
		throw;
	}
	_pDocument->resumeEvents();
	_pDocument->collectGarbage();
	return _pDocument;
}


void DOMBuilder::setupParse()
{
	_pDocument  = new Document(_pNamePool);
	_pParent    = _pDocument;
	_pPrevious  = 0;
	_inCDATA    = false;
	_namespaces = _xmlReader.getFeature(XMLReader::FEATURE_NAMESPACES);
}


inline void DOMBuilder::appendNode(AbstractNode* pNode)
{
	if (_pPrevious && _pPrevious != _pParent)
	{
		_pPrevious->_pNext = pNode;
		pNode->_pParent = _pParent;
		pNode->duplicate();
	}
	else _pParent->appendChild(pNode);
	_pPrevious = pNode;
}


void DOMBuilder::notationDecl(const XMLString& name, const XMLString* publicId, const XMLString* systemId)
{
	DocumentType* pDoctype = _pDocument->getDoctype();
	if (pDoctype)
	{
		AutoPtr<Notation> pNotation = _pDocument->createNotation(name, (publicId ? *publicId : EMPTY_STRING), (systemId ? *systemId : EMPTY_STRING));
		pDoctype->appendChild(pNotation);
	}
}


void DOMBuilder::unparsedEntityDecl(const XMLString& name, const XMLString* publicId, const XMLString& systemId, const XMLString& notationName)
{
	DocumentType* pDoctype = _pDocument->getDoctype();
	if (pDoctype)
	{
		AutoPtr<Entity> pEntity = _pDocument->createEntity(name, publicId ? *publicId : EMPTY_STRING, systemId, notationName);
		pDoctype->appendChild(pEntity);
	}
}


void DOMBuilder::setDocumentLocator(const Locator* loc)
{
}


void DOMBuilder::startDocument()
{
}


void DOMBuilder::endDocument()
{
}


void DOMBuilder::startElement(const XMLString& uri, const XMLString& localName, const XMLString& qname, const Attributes& attributes)
{
	AutoPtr<Element> pElem = _namespaces ? _pDocument->createElementNS(uri, qname.empty() ? localName : qname) : _pDocument->createElement(qname);

	const AttributesImpl& attrs = dynamic_cast<const AttributesImpl&>(attributes);
	Attr* pPrevAttr = 0;
	for (AttributesImpl::iterator it = attrs.begin(); it != attrs.end(); ++it)
	{
		AutoPtr<Attr> pAttr = new Attr(_pDocument, 0, it->namespaceURI, it->localName, it->qname, it->value, it->specified);
		pPrevAttr = pElem->addAttributeNodeNP(pPrevAttr, pAttr);
	}
	appendNode(pElem);
	_pParent = pElem;
}


void DOMBuilder::endElement(const XMLString& uri, const XMLString& localName, const XMLString& qname)
{
	_pPrevious = _pParent;
	_pParent   = static_cast<AbstractContainerNode*>(_pParent->parentNode());
}


void DOMBuilder::characters(const XMLChar ch[], int start, int length)
{
	if (_inCDATA)
	{
		if (_pPrevious && _pPrevious->nodeType() == Node::CDATA_SECTION_NODE)
		{
			static_cast<CDATASection*>(_pPrevious)->appendData(XMLString(ch + start, length));
		}
		else
		{
			AutoPtr<CDATASection> pCDATA = _pDocument->createCDATASection(XMLString(ch + start, length));
			appendNode(pCDATA);
		}
	}
	else
	{
		if (_pPrevious && _pPrevious->nodeType() == Node::TEXT_NODE)
		{
			static_cast<Text*>(_pPrevious)->appendData(XMLString(ch + start, length));
		}
		else
		{
			AutoPtr<Text> pText = _pDocument->createTextNode(XMLString(ch + start, length));
			appendNode(pText);
		}
	}
}


void DOMBuilder::ignorableWhitespace(const XMLChar ch[], int start, int length)
{
	characters(ch, start, length);
}


void DOMBuilder::processingInstruction(const XMLString& target, const XMLString& data)
{
	AutoPtr<ProcessingInstruction> pPI = _pDocument->createProcessingInstruction(target, data);
	appendNode(pPI);
}


void DOMBuilder::startPrefixMapping(const XMLString& prefix, const XMLString& uri)
{
}


void DOMBuilder::endPrefixMapping(const XMLString& prefix)
{
}


void DOMBuilder::skippedEntity(const XMLString& name)
{
	AutoPtr<EntityReference> pER = _pDocument->createEntityReference(name);
	appendNode(pER);
}


void DOMBuilder::startDTD(const XMLString& name, const XMLString& publicId, const XMLString& systemId)
{
	AutoPtr<DocumentType> pDoctype = new DocumentType(_pDocument, name, publicId, systemId);
	_pDocument->setDoctype(pDoctype);
}


void DOMBuilder::endDTD()
{
}


void DOMBuilder::startEntity(const XMLString& name)
{
}


void DOMBuilder::endEntity(const XMLString& name)
{
}


void DOMBuilder::startCDATA()
{
	_inCDATA = true;
}


void DOMBuilder::endCDATA()
{
	_inCDATA = false;
}


void DOMBuilder::comment(const XMLChar ch[], int start, int length)
{
	AutoPtr<Comment> pComment = _pDocument->createComment(XMLString(ch + start, length));
	appendNode(pComment);
}


} } // namespace Poco::XML
