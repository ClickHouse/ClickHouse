//
// DefaultHandler.h
//
// $Id: //poco/1.4/XML/include/Poco/SAX/DefaultHandler.h#1 $
//
// Library: XML
// Package: SAX
// Module:  SAX
//
// SAX-2 DefaultHandler class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef SAX_DefaultHandler_INCLUDED
#define SAX_DefaultHandler_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/SAX/EntityResolver.h"
#include "Poco/SAX/DTDHandler.h"
#include "Poco/SAX/ContentHandler.h"
#include "Poco/SAX/ErrorHandler.h"


namespace Poco {
namespace XML {


class XML_API DefaultHandler: public EntityResolver, public DTDHandler, public ContentHandler, public ErrorHandler
	/// Default base class for SAX2 event handlers. 
	/// This class is available as a convenience base class for SAX2 applications: 
	/// it provides default implementations for all of the
	/// callbacks in the four core SAX2 handler classes:
	///      * EntityResolver 
	///      * DTDHandler 
	///      * ContentHandler 
	///      * ErrorHandler 
	/// Application writers can extend this class when they need to implement only 
	/// part of an interface; parser writers can instantiate this
	/// class to provide default handlers when the application has not supplied its own.
{
public:
	DefaultHandler();
		/// Creates the DefaultHandler.
		
	~DefaultHandler();
		/// Destroys the DefaultHandler.
	
	// EntityResolver
	InputSource* resolveEntity(const XMLString* publicId, const XMLString& systemId);
	void releaseInputSource(InputSource* pSource);
	
	// DTDHandler
	void notationDecl(const XMLString& name, const XMLString* publicId, const XMLString* systemId);
	void unparsedEntityDecl(const XMLString& name, const XMLString* publicId, const XMLString& systemId, const XMLString& notationName);

	// ContentHandler
	void setDocumentLocator(const Locator* loc);
	void startDocument();
	void endDocument();
	void startElement(const XMLString& uri, const XMLString& localName, const XMLString& qname, const Attributes& attributes);
	void endElement(const XMLString& uri, const XMLString& localName, const XMLString& qname);
	void characters(const XMLChar ch[], int start, int length);
	void ignorableWhitespace(const XMLChar ch[], int start, int length);
	void processingInstruction(const XMLString& target, const XMLString& data);
	void startPrefixMapping(const XMLString& prefix, const XMLString& uri);
	void endPrefixMapping(const XMLString& prefix);
	void skippedEntity(const XMLString& name);
	
	// ErrorHandler
	void warning(const SAXException& exc);
	void error(const SAXException& exc);
	void fatalError(const SAXException& exc);
};


} } // namespace Poco::XML


#endif // SAX_DefaultHandler_INCLUDED
