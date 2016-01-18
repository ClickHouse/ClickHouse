//
// XMLFilterImpl.cpp
//
// $Id: //poco/1.4/XML/src/XMLFilterImpl.cpp#1 $
//
// Library: XML
// Package: SAX
// Module:  SAXFilters
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/SAX/XMLFilterImpl.h"
#include "Poco/SAX/SAXException.h"


namespace Poco {
namespace XML {


XMLFilterImpl::XMLFilterImpl():
	_pParent(0),
	_pEntityResolver(0),
	_pDTDHandler(0),
	_pContentHandler(0),
	_pErrorHandler(0)
{
}

	
XMLFilterImpl::XMLFilterImpl(XMLReader* pParent):
	_pParent(pParent),
	_pEntityResolver(0),
	_pDTDHandler(0),
	_pContentHandler(0),
	_pErrorHandler(0)
{
}

	
XMLFilterImpl::~XMLFilterImpl()
{
}


XMLReader* XMLFilterImpl::getParent() const
{
	return _pParent;
}


void XMLFilterImpl::setParent(XMLReader* pParent)
{
	_pParent = pParent;
}


void XMLFilterImpl::setEntityResolver(EntityResolver* pResolver)
{
	_pEntityResolver = pResolver;
}


EntityResolver* XMLFilterImpl::getEntityResolver() const
{
	return _pEntityResolver;
}


void XMLFilterImpl::setDTDHandler(DTDHandler* pDTDHandler)
{
	_pDTDHandler = pDTDHandler;
}


DTDHandler* XMLFilterImpl::getDTDHandler() const
{
	return _pDTDHandler;
}


void XMLFilterImpl::setContentHandler(ContentHandler* pContentHandler)
{
	_pContentHandler = pContentHandler;
}


ContentHandler* XMLFilterImpl::getContentHandler() const
{
	return _pContentHandler;
}


void XMLFilterImpl::setErrorHandler(ErrorHandler* pErrorHandler)
{
	_pErrorHandler = pErrorHandler;
}


ErrorHandler* XMLFilterImpl::getErrorHandler() const
{
	return _pErrorHandler;
}


void XMLFilterImpl::setFeature(const XMLString& featureId, bool state)
{
	if (_pParent)
		_pParent->setFeature(featureId, state);
	else
		throw SAXNotRecognizedException(fromXMLString(featureId));
}


bool XMLFilterImpl::getFeature(const XMLString& featureId) const
{
	if (_pParent)
		return _pParent->getFeature(featureId);
	else
		throw SAXNotRecognizedException(fromXMLString(featureId));
}


void XMLFilterImpl::setProperty(const XMLString& propertyId, const XMLString& value)
{
	if (_pParent)
		_pParent->setProperty(propertyId, value);
	else
		throw SAXNotRecognizedException(fromXMLString(propertyId));
}


void XMLFilterImpl::setProperty(const XMLString& propertyId, void* value)
{
	if (_pParent)
		_pParent->setProperty(propertyId, value);
	else
		throw SAXNotRecognizedException(fromXMLString(propertyId));
}


void* XMLFilterImpl::getProperty(const XMLString& propertyId) const
{
	if (_pParent)
		return _pParent->getProperty(propertyId);
	else
		throw SAXNotRecognizedException(fromXMLString(propertyId));
}


void XMLFilterImpl::parse(InputSource* pSource)
{
	setupParse();
	_pParent->parse(pSource);
}


void XMLFilterImpl::parse(const XMLString& systemId)
{
	setupParse();
	_pParent->parse(systemId);
}


void XMLFilterImpl::parseMemoryNP(const char* xml, std::size_t size)
{
	setupParse();
	_pParent->parseMemoryNP(xml, size);
}


InputSource* XMLFilterImpl::resolveEntity(const XMLString* publicId, const XMLString& systemId)
{
	if (_pEntityResolver)
		return _pEntityResolver->resolveEntity(publicId, systemId);
	else
		return 0;
}


void XMLFilterImpl::releaseInputSource(InputSource* pSource)
{
	if (_pEntityResolver)
		_pEntityResolver->releaseInputSource(pSource);
}


void XMLFilterImpl::notationDecl(const XMLString& name, const XMLString* publicId, const XMLString* systemId)
{
	if (_pDTDHandler)
		_pDTDHandler->notationDecl(name, publicId, systemId);
}


void XMLFilterImpl::unparsedEntityDecl(const XMLString& name, const XMLString* publicId, const XMLString& systemId, const XMLString& notationName)
{
	if (_pDTDHandler)
		_pDTDHandler->unparsedEntityDecl(name, publicId, systemId, notationName);
}


void XMLFilterImpl::setDocumentLocator(const Locator* loc)
{
	if (_pContentHandler)
		_pContentHandler->setDocumentLocator(loc);
}


void XMLFilterImpl::startDocument()
{
	if (_pContentHandler)
		_pContentHandler->startDocument();
}


void XMLFilterImpl::endDocument()
{
	if (_pContentHandler)
		_pContentHandler->endDocument();
}


void XMLFilterImpl::startElement(const XMLString& uri, const XMLString& localName, const XMLString& qname, const Attributes& attrList)
{
	if (_pContentHandler)
		_pContentHandler->startElement(uri, localName, qname, attrList);
}


void XMLFilterImpl::endElement(const XMLString& uri, const XMLString& localName, const XMLString& qname)
{
	if (_pContentHandler)
		_pContentHandler->endElement(uri, localName, qname);
}


void XMLFilterImpl::characters(const XMLChar ch[], int start, int length)
{
	if (_pContentHandler)
		_pContentHandler->characters(ch, start, length);
}


void XMLFilterImpl::ignorableWhitespace(const XMLChar ch[], int start, int length)
{
	if (_pContentHandler)
		_pContentHandler->ignorableWhitespace(ch, start, length);
}


void XMLFilterImpl::processingInstruction(const XMLString& target, const XMLString& data)
{
	if (_pContentHandler)
		_pContentHandler->processingInstruction(target, data);
}


void XMLFilterImpl::startPrefixMapping(const XMLString& prefix, const XMLString& uri)
{
	if (_pContentHandler)
		_pContentHandler->startPrefixMapping(prefix, uri);
}


void XMLFilterImpl::endPrefixMapping(const XMLString& prefix)
{
	if (_pContentHandler)
		_pContentHandler->endPrefixMapping(prefix);
}


void XMLFilterImpl::skippedEntity(const XMLString& prefix)
{
	if (_pContentHandler)
		_pContentHandler->skippedEntity(prefix);
}


void XMLFilterImpl::warning(const SAXException& e)
{
	if (_pErrorHandler)
		_pErrorHandler->warning(e);
}


void XMLFilterImpl::error(const SAXException& e)
{
	if (_pErrorHandler)
		_pErrorHandler->error(e);
}


void XMLFilterImpl::fatalError(const SAXException& e)
{
	if (_pErrorHandler)
		_pErrorHandler->fatalError(e);
}


void XMLFilterImpl::setupParse()
{
	poco_check_ptr (_pParent);

	_pParent->setEntityResolver(this);
	_pParent->setDTDHandler(this);
	_pParent->setContentHandler(this);
	_pParent->setErrorHandler(this);
}


} } // namespace Poco::XML
