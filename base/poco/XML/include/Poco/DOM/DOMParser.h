//
// DOMParser.h
//
// Library: XML
// Package: DOM
// Module:  DOMParser
//
// Definition of the DOMParser class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DOM_DOMParser_INCLUDED
#define DOM_DOMParser_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/SAX/SAXParser.h"


namespace Poco {
namespace XML {


class NamePool;
class Document;
class InputSource;
class EntityResolver;


class XML_API DOMParser
	/// This is a convenience class that combines a
	/// DOMBuilder with a SAXParser, with the optional
	/// support of a WhitespaceFilter.
{
public:
	explicit DOMParser(NamePool* pNamePool = 0);
		/// Creates a new DOMParser. 
		/// If a NamePool is given, it becomes the Document's NamePool.
		
	explicit DOMParser(unsigned long namePoolSize);
		/// Creates a new DOMParser, using the given NamePool size.
		///
		/// The given namePoolSize should be a suitable prime number, 
		/// e.g. 251, 509, 1021 or 4093, depending on the expected
		/// size of the document.

	~DOMParser();
		/// Destroys the DOMParser.

	void setEncoding(const XMLString& encoding);
		/// Sets the encoding used by the parser if no
		/// encoding is specified in the XML document.
		
	const XMLString& getEncoding() const;
		/// Returns the name of the encoding used by
		/// the parser if no encoding is specified in
		/// the XML document.

	void addEncoding(const XMLString& name, Poco::TextEncoding* pEncoding);
		/// Adds an encoding to the parser.

	void setFeature(const XMLString& name, bool state);
		/// Set the state of a feature.
		///
		/// If a feature is not recognized by the DOMParser, it is
		/// passed on to the underlying XMLReader.
		///
		/// The only currently supported feature is
		/// http://www.appinf.com/features/no-whitespace-in-element-content
		/// which, when activated, causes the WhitespaceFilter to
		/// be used.

	bool getFeature(const XMLString& name) const;
		/// Look up the value of a feature.
		///
		/// If a feature is not recognized by the DOMParser, the
		/// DOMParser queries the underlying SAXParser for the feature.

	Document* parse(const XMLString& uri);
		/// Parse an XML document from a location identified by an URI.

	Document* parse(InputSource* pInputSource);
		/// Parse an XML document from a location identified by an InputSource.

	Document* parseString(const std::string& xml);
		/// Parse an XML document from a string.

	Document* parseMemory(const char* xml, std::size_t size);
		/// Parse an XML document from memory.

	EntityResolver* getEntityResolver() const;
		/// Returns the entity resolver used by the underlying SAXParser.

	void setEntityResolver(EntityResolver* pEntityResolver);
		/// Sets the entity resolver on the underlying SAXParser.

	static const XMLString FEATURE_FILTER_WHITESPACE;
	
private:
	SAXParser _saxParser;
	NamePool* _pNamePool;
	bool      _filterWhitespace;
};


} } // namespace Poco::XML


#endif // DOM_DOMParser_INCLUDED
