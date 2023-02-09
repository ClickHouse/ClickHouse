//
// XMLFilterImpl.h
//
// Library: XML
// Package: SAX
// Module:  SAXFilters
//
// SAX2 XMLFilterImpl class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef SAX_XMLFilterImpl_INCLUDED
#define SAX_XMLFilterImpl_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/SAX/XMLFilter.h"
#include "Poco/SAX/EntityResolver.h"
#include "Poco/SAX/DTDHandler.h"
#include "Poco/SAX/ContentHandler.h"
#include "Poco/SAX/ErrorHandler.h"


namespace Poco {
namespace XML {


class XML_API XMLFilterImpl: public XMLFilter, public EntityResolver, public DTDHandler, public ContentHandler, public ErrorHandler
	/// Base class for deriving an XML filter.
	///
	/// This class is designed to sit between an XMLReader and the client application's event 
	/// handlers. By default, it does nothing but pass requests up to the reader and events on to 
	/// the handlers unmodified, but subclasses can override specific methods to modify the event 
	/// stream or the configuration requests as they pass through.
{
public:
	XMLFilterImpl();
		/// Construct an empty XML filter, with no parent.
		///
		/// This filter will have no parent: you must assign a parent before you start a parse or do any 
		/// configuration with setFeature or setProperty, unless you use this as a pure event consumer rather 
		/// than as an XMLReader.
		
	XMLFilterImpl(XMLReader* pParent);
		/// Construct an XML filter with the specified parent.
		
	~XMLFilterImpl();
		/// Destroys the XMLFilterImpl.
	
	// XMLFilter
	XMLReader* getParent() const;
	void setParent(XMLReader* pParent);

	// XMLReader
	void setEntityResolver(EntityResolver* pResolver);
	EntityResolver* getEntityResolver() const;
	void setDTDHandler(DTDHandler* pDTDHandler);
	DTDHandler* getDTDHandler() const;
	void setContentHandler(ContentHandler* pContentHandler);
	ContentHandler* getContentHandler() const;
	void setErrorHandler(ErrorHandler* pErrorHandler);
	ErrorHandler* getErrorHandler() const;
	void setFeature(const XMLString& featureId, bool state);
	bool getFeature(const XMLString& featureId) const;
	void setProperty(const XMLString& propertyId, const XMLString& value);
	void setProperty(const XMLString& propertyId, void* value);
	void* getProperty(const XMLString& propertyId) const;
	void parse(InputSource* pSource);
	void parse(const XMLString& systemId);
	void parseMemoryNP(const char* xml, std::size_t size);

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
	void startElement(const XMLString& uri, const XMLString& localName, const XMLString& qname, const Attributes& attrList);
	void endElement(const XMLString& uri, const XMLString& localName, const XMLString& qname);
	void characters(const XMLChar ch[], int start, int length);
	void ignorableWhitespace(const XMLChar ch[], int start, int length);
	void processingInstruction(const XMLString& target, const XMLString& data);
	void startPrefixMapping(const XMLString& prefix, const XMLString& uri);
	void endPrefixMapping(const XMLString& prefix);
	void skippedEntity(const XMLString& prefix);
	void warning(const SAXException& e);
	void error(const SAXException& e);
	void fatalError(const SAXException& e);

protected:
	XMLReader* parent() const;
		/// Return a pointer to the parent reader.
		/// Subclasses can use this method instead of
		/// getParent() for better performance - this method
		/// is non-virtual and implemented as inline.
	
	virtual void setupParse();
		/// Setup the event handlers in the parent reader.
	
private:
	XMLReader*      _pParent;
	EntityResolver* _pEntityResolver;
	DTDHandler*     _pDTDHandler;
	ContentHandler* _pContentHandler;
	ErrorHandler*   _pErrorHandler;
};


//
// inlines
//
inline XMLReader* XMLFilterImpl::parent() const
{
	return _pParent;
}


} } // namespace Poco::XML


#endif // SAX_XMLFilterImpl_INCLUDED
