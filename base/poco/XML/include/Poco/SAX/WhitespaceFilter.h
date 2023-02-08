//
// WhitespaceFilter.h
//
// Library: XML
// Package: SAX
// Module:  WhitespaceFilter
//
// Definition of the WhitespaceFilter class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef SAX_WhitespaceFilter_INCLUDED
#define SAX_WhitespaceFilter_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/SAX/XMLFilterImpl.h"
#include "Poco/SAX/LexicalHandler.h"


namespace Poco {
namespace XML {


class XML_API WhitespaceFilter: public XMLFilterImpl, public LexicalHandler
	/// This implementation of the SAX2 XMLFilter interface
	/// filters all whitespace-only character data element
	/// content.
{
public:
	WhitespaceFilter();
		/// Creates the WhitespaceFilter, with no parent.
		
	WhitespaceFilter(XMLReader* pReader);
		/// Creates the WhitespaceFilter with the specified parent.

	~WhitespaceFilter();
		/// Destroys the WhitespaceFilter.

	// XMLReader
	void setProperty(const XMLString& propertyId, const XMLString& value);
	void setProperty(const XMLString& propertyId, void* value);
	void* getProperty(const XMLString& propertyId) const;

	// ContentHandler
	void startDocument();
	void endDocument();
	void startElement(const XMLString& uri, const XMLString& localName, const XMLString& qname, const Attributes& attrList);
	void endElement(const XMLString& uri, const XMLString& localName, const XMLString& qname);
	void characters(const XMLChar ch[], int start, int length);
	void ignorableWhitespace(const XMLChar ch[], int start, int length);
	void processingInstruction(const XMLString& target, const XMLString& data);

	// LexicalHandler
	void startDTD(const XMLString& name, const XMLString& publicId, const XMLString& systemId);
	void endDTD();
	void startEntity(const XMLString& name);
	void endEntity(const XMLString& name);
	void startCDATA();
	void endCDATA();
	void comment(const XMLChar ch[], int start, int length);

protected:
	void setupParse();

private:
	LexicalHandler* _pLexicalHandler;
	XMLString       _data;
	bool            _filter;
};


} } // namespace Poco::XML


#endif // SAX_WhitespaceFilter_INCLUDED
