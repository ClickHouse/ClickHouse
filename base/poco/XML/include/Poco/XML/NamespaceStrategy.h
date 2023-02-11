//
// NamespaceStrategy.h
//
// Library: XML
// Package: XML
// Module:  NamespaceStrategy
//
// Definition of the NamespaceStrategy class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef XML_NamespaceStrategy_INCLUDED
#define XML_NamespaceStrategy_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/XML/XMLString.h"
#include "Poco/SAX/NamespaceSupport.h"
#include "Poco/SAX/AttributesImpl.h"


namespace Poco {
namespace XML {


class ContentHandler;


class XML_API NamespaceStrategy
	/// This class is used by ParserEngine to handle the
	/// startElement, endElement, startPrefixMapping and
	/// endPrefixMapping events.
{
public:
	virtual ~NamespaceStrategy();

	virtual void startElement(const XMLChar* name, const XMLChar** atts, int specifiedCount, ContentHandler* pContentHandler) = 0;
		/// Translate the arguments as delivered by Expat and
		/// call the startElement() method of the ContentHandler.
		
	virtual void endElement(const XMLChar* name, ContentHandler* pContentHandler) = 0;
		/// Translate the arguments as delivered by Expat and
		/// call the endElement() method of the ContentHandler.

protected:
	static void splitName(const XMLChar* qname, XMLString& uri, XMLString& localName);
	static void splitName(const XMLChar* qname, XMLString& uri, XMLString& localName, XMLString& prefix);

	static const XMLString NOTHING;
};


class XML_API NoNamespacesStrategy: public NamespaceStrategy
	/// The NamespaceStrategy implementation used if no namespaces
	/// processing is requested.
{
public:
	NoNamespacesStrategy();
	~NoNamespacesStrategy();
	
	void startElement(const XMLChar* name, const XMLChar** atts, int specifiedCount, ContentHandler* pContentHandler);
	void endElement(const XMLChar* name, ContentHandler* pContentHandler);
	
private:
	XMLString _name;
	AttributesImpl _attrs;
};


class XML_API NoNamespacePrefixesStrategy: public NamespaceStrategy
	/// The NamespaceStrategy implementation used if namespaces
	/// processing is requested, but prefixes are not reported.
{
public:
	NoNamespacePrefixesStrategy();
	~NoNamespacePrefixesStrategy();
	
	void startElement(const XMLChar* name, const XMLChar** atts, int specifiedCount, ContentHandler* pContentHandler);
	void endElement(const XMLChar* name, ContentHandler* pContentHandler);

private:
	XMLString _uri;
	XMLString _local;
	AttributesImpl _attrs;
};


class XML_API NamespacePrefixesStrategy: public NamespaceStrategy
	/// The NamespaceStrategy implementation used if namespaces
	/// processing is requested and prefixes are reported.
{
public:
	NamespacePrefixesStrategy();
	~NamespacePrefixesStrategy();
	
	void startElement(const XMLChar* name, const XMLChar** atts, int specifiedCount, ContentHandler* pContentHandler);
	void endElement(const XMLChar* name, ContentHandler* pContentHandler);
	
private:
	XMLString _uri;
	XMLString _local;
	XMLString _qname;
	AttributesImpl _attrs;
};


} } // namespace Poco::XML


#endif // XML_NamespaceStrategy_INCLUDED
