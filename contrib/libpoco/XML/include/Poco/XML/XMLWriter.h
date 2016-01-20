//
// XMLWriter.h
//
// $Id: //poco/1.4/XML/include/Poco/XML/XMLWriter.h#3 $
//
// Library: XML
// Package: XML
// Module:  XMLWriter
//
// Definition of the XMLWriter class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef XML_XMLWriter_INCLUDED
#define XML_XMLWriter_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/SAX/ContentHandler.h"
#include "Poco/SAX/LexicalHandler.h"
#include "Poco/SAX/DTDHandler.h"
#include "Poco/SAX/NamespaceSupport.h"
#include "Poco/XML/XMLString.h"
#include "Poco/XML/XMLStream.h"
#include "Poco/XML/Name.h"
#include "Poco/TextEncoding.h"
#include "Poco/StreamConverter.h"
#include <vector>
#include <map>


namespace Poco {
namespace XML {


class Locator;


class XML_API XMLWriter: public ContentHandler, public LexicalHandler, public DTDHandler
	/// This class serializes SAX2 ContentHandler, LexicalHandler and
	/// DTDHandler events back into a stream.
	/// 
	/// Various consistency checks are performed on the written data
	/// (i.e. there must be exactly one root element and every startElement() 
	/// must have a matching endElement()).
	///
	/// The XMLWriter supports optional pretty-printing of the serialized XML.
	/// Note, however, that pretty-printing XML data alters the
	/// information set of the document being written, since in
	/// XML all whitespace is potentially relevant to an application.
	///
	/// The writer contains extensive support for XML Namespaces, so that a client  
	/// application does not have to keep track of prefixes and supply xmlns attributes.
	///
	/// If the client does not provide namespace prefixes (either by specifying them
	/// as part of the qualified name given to startElement(), or by calling
	/// startPrefixMapping()), the XMLWriter automatically generates namespace
	/// prefixes in the form ns1, ns2, etc.
{
public:
	enum Options
	{
		CANONICAL               = 0x00,
			/// Do not write an XML declaration (default).

		CANONICAL_XML           = 0x01, 
			/// Enables basic support for Canonical XML: 
			///   - do not write an XML declaration
			///   - do not use special empty element syntax
			///   - set the New Line character to NEWLINE_LF

		WRITE_XML_DECLARATION   = 0x02, 
			/// Write an XML declaration.

		PRETTY_PRINT            = 0x04, 
			/// Pretty-print XML markup.

		PRETTY_PRINT_ATTRIBUTES = 0x08
			/// Write each attribute on a separate line. 
			/// PRETTY_PRINT must be specified as well.
	};

	XMLWriter(XMLByteOutputStream& str, int options);
		/// Creates the XMLWriter and sets the specified options.
		///
		/// The resulting stream will be UTF-8 encoded.

	XMLWriter(XMLByteOutputStream& str, int options, const std::string& encodingName, Poco::TextEncoding& textEncoding);
		/// Creates the XMLWriter and sets the specified options.
		///
		/// The encoding is reflected in the XML declaration.
		/// The caller is responsible for that the given encodingName matches with
		/// the given textEncoding.

	XMLWriter(XMLByteOutputStream& str, int options, const std::string& encodingName, Poco::TextEncoding* pTextEncoding);
		/// Creates the XMLWriter and sets the specified options.
		///
		/// The encoding is reflected in the XML declaration.
		/// The caller is responsible for that the given encodingName matches with
		/// the given textEncoding.
		/// If pTextEncoding is null, the given encodingName is ignored and the
		/// default UTF-8 encoding is used.

	~XMLWriter();
		/// Destroys the XMLWriter.

	void setNewLine(const std::string& newLineCharacters);
		/// Sets the line ending for the resulting XML file.
		///
		/// Possible values are:
		///   * NEWLINE_DEFAULT (whatever is appropriate for the current platform)
		///   * NEWLINE_CRLF    (Windows),
		///   * NEWLINE_LF      (Unix),
		///   * NEWLINE_CR      (Macintosh)

	const std::string& getNewLine() const;
		/// Returns the line ending currently in use.

	void setIndent(const std::string& indent);
		/// Sets the string used for one indentation step.
		///
		/// The default is a single TAB character.
		/// The given string should only contain TAB or SPACE
		/// characters (e.g., a single TAB character, or
		/// two to four SPACE characters).
		
	const std::string& getIndent() const;
		/// Returns the string used for one indentation step.

	// ContentHandler
	void setDocumentLocator(const Locator* loc);
		/// Currently unused.

	void startDocument();
		/// Writes a generic XML declaration to the stream.
		/// If a document type has been set (see SetDocumentType),
		/// a DOCTYPE declaration is also written.

	void endDocument();
		/// Checks that all elements are closed and prints a final newline.

	void startFragment();
		/// Use this instead of StartDocument() if you want to write
		/// a fragment rather than a document (no XML declaration and
		/// more than one "root" element allowed).

	void endFragment();
		/// Checks that all elements are closed and prints a final newline.

	void startElement(const XMLString& namespaceURI, const XMLString& localName, const XMLString& qname, const Attributes& attributes);
		/// Writes an XML start element tag.
		///
		/// Namespaces are handled as follows.
		///   1. If a qname, but no namespaceURI and localName are given, the qname is taken as element name.
		///   2. If a namespaceURI and a localName, but no qname is given, and the given namespaceURI has been
		///      declared earlier, the namespace prefix for the given namespaceURI together with the localName
		///      is taken as element name. If the namespace has not been declared, a prefix in the form
		///      "ns1", "ns2", etc. is generated and the namespace is declared with the generated prefix.
		///   3. If all three are given, and the namespace given in namespaceURI has not been declared, it is declared now.
		///      Otherwise, see 2.

	void startElement(const XMLString& namespaceURI, const XMLString& localName, const XMLString& qname);
		/// Writes an XML start element tag with no attributes.
		/// See the other startElement() method for more information.
		
	void endElement(const XMLString& namespaceURI, const XMLString& localName, const XMLString& qname);
		/// Writes an XML end element tag.
		///
		/// Throws an exception if the name of doesn't match the
		/// one of the most recent startElement().

	void emptyElement(const XMLString& namespaceURI, const XMLString& localName, const XMLString& qname);
		/// Writes an empty XML element tag (<elem/>).

	void emptyElement(const XMLString& namespaceURI, const XMLString& localName, const XMLString& qname, const Attributes& attributes);
		/// Writes an empty XML element tag with the given attributes (<elem attr1="value1"... />).

	void characters(const XMLChar ch[], int start, int length);
		/// Writes XML character data. Quotes, ampersand's, less-than and
		/// greater-than signs are escaped, unless a CDATA section
		/// has been opened by calling startCDATA().
		///
		/// The characters must be encoded in UTF-8 (if XMLChar is char) or 
		/// UTF-16 (if XMLChar is wchar_t).

	void characters(const XMLString& str);
		/// Writes XML character data. Quotes, ampersand's, less-than and
		/// greater-than signs are escaped, unless a CDATA section
		/// has been opened by calling startCDATA().
		///
		/// The characters must be encoded in UTF-8 (if XMLChar is char) or 
		/// UTF-16 (if XMLChar is wchar_t).

	void rawCharacters(const XMLString& str);
		/// Writes the characters in the given string as they are.
		/// The caller is responsible for escaping characters as
		/// necessary to produce valid XML.
		///
		/// The characters must be encoded in UTF-8 (if XMLChar is char) or 
		/// UTF-16 (if XMLChar is wchar_t).

	void ignorableWhitespace(const XMLChar ch[], int start, int length);
		/// Writes whitespace characters by simply passing them to
		/// characters().

	void processingInstruction(const XMLString& target, const XMLString& data);
		/// Writes a processing instruction.

	void startPrefixMapping(const XMLString& prefix, const XMLString& namespaceURI);
		/// Begin the scope of a prefix-URI Namespace mapping.
		/// A namespace declaration is written with the next element.

	void endPrefixMapping(const XMLString& prefix);
		/// End the scope of a prefix-URI mapping.

	void skippedEntity(const XMLString& name);
		/// Does nothing.

	void dataElement(const XMLString& namespaceURI, const XMLString& localName, const XMLString& qname, const XMLString& data, 
	                         const XMLString& attr1 = XMLString(), const XMLString& value1 = XMLString(),
							 const XMLString& attr2 = XMLString(), const XMLString& value2 = XMLString(),
							 const XMLString& attr3 = XMLString(), const XMLString& value3 = XMLString());
		/// Writes a data element in the form <name attr1="value1"...>data</name>.

	// LexicalHandler
	void startCDATA();
		/// Writes the <![CDATA[ string that begins a CDATA section.
		/// Use characters() to write the actual character data.

	void endCDATA();
		/// Writes the ]]> string that ends a CDATA section.

	void comment(const XMLChar ch[], int start, int length);
		/// Writes a comment.

	void startDTD(const XMLString& name, const XMLString& publicId, const XMLString& systemId);
		/// Writes a DTD declaration.

	void endDTD();
		/// Writes the closing characters of a DTD declaration.

	void startEntity(const XMLString& name);
		/// Does nothing.

	void endEntity(const XMLString& name);
		/// Does nothing.
	
	// DTDHandler
	void notationDecl(const XMLString& name, const XMLString* publicId, const XMLString* systemId);
	void unparsedEntityDecl(const XMLString& name, const XMLString* publicId, const XMLString& systemId, const XMLString& notationName);

	static const std::string NEWLINE_DEFAULT;
	static const std::string NEWLINE_CR;
	static const std::string NEWLINE_CRLF;
	static const std::string NEWLINE_LF;

	// Namespace support.
	XMLString uniquePrefix();
		/// Creates and returns a unique namespace prefix that
		/// can be used with startPrefixMapping().

	bool isNamespaceMapped(const XMLString& namespc) const;
		/// Returns true if the given namespace has been mapped
		/// to a prefix in the current element or its ancestors.
		
	// Misc.
	int depth() const;
		/// Return the number of nested XML elements.
		///
		/// Will be -1 if no document or fragment has been started, 
		/// 0 if the document or fragment has been started,
		/// 1 if the document element has been written and
		/// > 1 for every element nested within the document element.

protected:
	typedef std::map<XMLString, XMLString> AttributeMap;

	void writeStartElement(const XMLString& namespaceURI, const XMLString& localName, const XMLString& qname, const Attributes& attributes);
	void writeEndElement(const XMLString& namespaceURI, const XMLString& localName, const XMLString& qname);
	void writeMarkup(const std::string& str) const;
	void writeXML(const XMLString& str) const;
	void writeXML(XMLChar ch) const;
	void writeNewLine() const;
	void writeIndent() const;
	void writeIndent(int indent) const;
	void writeName(const XMLString& prefix, const XMLString& localName);
	void writeXMLDeclaration();
	void closeStartTag();
	void declareAttributeNamespaces(const Attributes& attributes);
	void addNamespaceAttributes(AttributeMap& attributeMap);
	void addAttributes(AttributeMap& attributeMap, const Attributes& attributes, const XMLString& elementNamespaceURI);
	void writeAttributes(const AttributeMap& attributeMap);
	void prettyPrint() const;
	static std::string nameToString(const XMLString& localName, const XMLString& qname);

private:
	struct Namespace
	{
		Namespace(const XMLString& thePrefix, const XMLString& theNamespaceURI):
			prefix(thePrefix),
			namespaceURI(theNamespaceURI)
		{
		}

		XMLString prefix;
		XMLString namespaceURI;
	};
	typedef std::vector<Name> ElementStack;
	
	Poco::OutputStreamConverter* _pTextConverter;
	Poco::TextEncoding*          _pInEncoding;
	Poco::TextEncoding*          _pOutEncoding;
	int              _options;
	std::string      _encoding;
	std::string      _newLine;
	int              _depth;
	int              _elementCount;
	bool             _inFragment;
	bool             _inCDATA;
	bool             _inDTD;
	bool             _inInternalDTD;
	bool             _contentWritten;
	bool             _unclosedStartTag;
	ElementStack     _elementStack;
	NamespaceSupport _namespaces;
	int              _prefix;
	bool             _nsContextPushed;
	std::string      _indent;

	static const std::string MARKUP_QUOTENC;
	static const std::string MARKUP_AMPENC;
	static const std::string MARKUP_LTENC;
	static const std::string MARKUP_GTENC;
	static const std::string MARKUP_TABENC;
	static const std::string MARKUP_CRENC;
	static const std::string MARKUP_LFENC;
	static const std::string MARKUP_LT;
	static const std::string MARKUP_GT;
	static const std::string MARKUP_SLASHGT;
	static const std::string MARKUP_LTSLASH;
	static const std::string MARKUP_COLON;
	static const std::string MARKUP_EQQUOT;
	static const std::string MARKUP_QUOT;
	static const std::string MARKUP_SPACE;
	static const std::string MARKUP_TAB;
	static const std::string MARKUP_BEGIN_CDATA;
	static const std::string MARKUP_END_CDATA;
};


//
// inlines
//
inline int XMLWriter::depth() const
{
	return _depth;
}


} } // namespace Poco::XML


#endif // XML_XMLWriter_INCLUDED
