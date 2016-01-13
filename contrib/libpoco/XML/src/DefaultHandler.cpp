//
// DefaultHandler.cpp
//
// $Id: //poco/1.4/XML/src/DefaultHandler.cpp#1 $
//
// Library: XML
// Package: SAX
// Module:  SAX
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/SAX/DefaultHandler.h"


namespace Poco {
namespace XML {


DefaultHandler::DefaultHandler()
{
}


DefaultHandler::~DefaultHandler()
{
}


InputSource* DefaultHandler::resolveEntity(const XMLString* publicId, const XMLString& systemId)
{
	return 0;
}


void DefaultHandler::releaseInputSource(InputSource* pSource)
{
}


void DefaultHandler::notationDecl(const XMLString& name, const XMLString* publicId, const XMLString* systemId)
{
}


void DefaultHandler::unparsedEntityDecl(const XMLString& name, const XMLString* publicId, const XMLString& systemId, const XMLString& notationName)
{
}


void DefaultHandler::setDocumentLocator(const Locator* loc)
{
}


void DefaultHandler::startDocument()
{
}


void DefaultHandler::endDocument()
{
}


void DefaultHandler::startElement(const XMLString& uri, const XMLString& localName, const XMLString& qname, const Attributes& attributes)
{
}


void DefaultHandler::endElement(const XMLString& uri, const XMLString& localName, const XMLString& qname)
{
}


void DefaultHandler::characters(const XMLChar ch[], int start, int length)
{
}


void DefaultHandler::ignorableWhitespace(const XMLChar ch[], int start, int length)
{
}


void DefaultHandler::processingInstruction(const XMLString& target, const XMLString& data)
{
}


void DefaultHandler::startPrefixMapping(const XMLString& prefix, const XMLString& uri)
{
}


void DefaultHandler::endPrefixMapping(const XMLString& prefix)
{
}


void DefaultHandler::skippedEntity(const XMLString& name)
{
}


void DefaultHandler::warning(const SAXException& exc)
{
}


void DefaultHandler::error(const SAXException& exc)
{
}


void DefaultHandler::fatalError(const SAXException& exc)
{
}


} } // namespace Poco::XML
