//
// ContentHandler.h
//
// Library: XML
// Package: SAX
// Module:  SAX
//
// SAX2 ContentHandler Interface.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef SAX_ContentHandler_INCLUDED
#define SAX_ContentHandler_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/XML/XMLString.h"


namespace Poco {
namespace XML {


class Locator;
class Attributes;


class XML_API ContentHandler
	/// Receive notification of the logical content of a document. 
	///
	/// This is the main interface that most SAX applications implement: if the
	/// application needs to be informed of basic parsing events, it implements
	/// this interface and registers an instance with the SAX parser using the setContentHandler
	/// method. The parser uses the instance to report basic document-related events
	/// like the start and end of elements and character data.
	/// 
	/// The order of events in this interface is very important, and mirrors the
	/// order of information in the document itself. For example, all of an element's
	/// content (character data, processing instructions, and/or subelements) will
	/// appear, in order, between the startElement event and the corresponding endElement
	/// event.
	/// 
	/// This interface is similar to the now-deprecated SAX 1.0 DocumentHandler
	/// interface, but it adds support for Namespaces and for reporting skipped
	/// entities (in non-validating XML processors).
	/// Receive notification of the logical content of a document.
{
public:
	virtual void setDocumentLocator(const Locator* loc) = 0;
		/// Receive an object for locating the origin of SAX document events.
		/// 
		/// SAX parsers are strongly encouraged (though not absolutely required) to
		/// supply a locator: if it does so, it must supply the locator to the application
		/// by invoking this method before invoking any of the other methods in the
		/// ContentHandler interface.
		/// 
		/// The locator allows the application to determine the end position of any
		/// document-related event, even if the parser is not reporting an error. Typically,
		/// the application will use this information for reporting its own errors (such
		/// as character content that does not match an application's business rules).
		/// The information returned by the locator is probably not sufficient for use
		/// with a search engine.
		/// 
		/// Note that the locator will return correct information only during the invocation
		/// SAX event callbacks after startDocument returns and before endDocument is
		/// called. The application should not attempt to use it at any other time.

	virtual void startDocument() = 0;
		/// Receive notification of the beginning of a document.
		///
		/// The SAX parser calls this function one time before calling all other 
		/// functions of this class (except SetDocumentLocator).

	virtual void endDocument() = 0;
		/// Receive notification of the end of a document.
		///
		/// The SAX parser will invoke this method only once, and it will be the last
		/// method invoked during the parse. The parser shall not invoke this method
		/// until it has either abandoned parsing (because of an unrecoverable error)
		/// or reached the end of input.

	virtual void startElement(const XMLString& uri, const XMLString& localName, const XMLString& qname, const Attributes& attrList) = 0;
		/// Receive notification of the beginning of an element.
		/// 
		/// The Parser will invoke this method at the beginning of every element in
		/// the XML document; there will be a corresponding endElement event for every
		/// startElement event (even when the element is empty). All of the element's
		/// content will be reported, in order, before the corresponding endElement
		/// event.
		/// 
		/// This event allows up to three name components for each element:
		///    1. the Namespace URI;
		///    2. the local name; and
		///    3. the qualified (prefixed) name.
		/// 
		/// Any or all of these may be provided, depending on the values of the http://xml.org/sax/features/namespaces
		/// and the http://xml.org/sax/features/namespace-prefixes properties:
		///     * the Namespace URI and local name are required when the namespaces
		///       property is true (the default), and are optional when the namespaces property
		///       is false (if one is specified, both must be);
		///     * the qualified name is required when the namespace-prefixes property
		///       is true, and is optional when the namespace-prefixes property is false (the
		///       default).
		/// 
		/// Note that the attribute list provided will contain only attributes with
		/// explicit values (specified or defaulted): #IMPLIED attributes will be omitted.
		/// The attribute list will contain attributes used for Namespace declarations
		/// (xmlns* attributes) only if the http://xml.org/sax/features/namespace-prefixes
		/// property is true (it is false by default, and support for a true value is
		/// optional).
		/// 
		/// Like characters(), attribute values may have characters that need more than
		/// one char value.

	virtual void endElement(const XMLString& uri, const XMLString& localName, const XMLString& qname) = 0;
		/// Receive notification of the end of an element.
		/// 
		/// The SAX parser will invoke this method at the end of every element in the
		/// XML document; there will be a corresponding startElement event for every
		/// endElement event (even when the element is empty).
		/// 
		/// For information on the names, see startElement.

	virtual void characters(const XMLChar ch[], int start, int length) = 0;
		/// Receive notification of character data.
		/// 
		/// The Parser will call this method to report each chunk of character data.
		/// SAX parsers may return all contiguous character data in a single chunk,
		/// or they may split it into several chunks; however, all of the characters
		/// in any single event must come from the same external entity so that the
		/// Locator provides useful information.
		/// 
		/// The application must not attempt to read from the array outside of the specified
		/// range.
		/// 
		/// Individual characters may consist of more than one XMLChar value. There
		/// are three important cases where this happens, because characters can't be
		/// represented in just sixteen bits. In one case, characters are represented
		/// in a Surrogate Pair, using two special Unicode values. Such characters are
		/// in the so-called "Astral Planes", with a code point above U+FFFF. A second
		/// case involves composite characters, such as a base character combining with
		/// one or more accent characters. And most important, if XMLChar is a plain
		/// char, characters are encoded in UTF-8.
		/// 
		/// Your code should not assume that algorithms using char-at-a-time idioms
		/// will be working in character units; in some cases they will split characters.
		/// This is relevant wherever XML permits arbitrary characters, such as attribute
		/// values, processing instruction data, and comments as well as in data reported
		/// from this method. It's also generally relevant whenever C++ code manipulates
		/// internationalized text; the issue isn't unique to XML.
		/// 
		/// Note that some parsers will report whitespace in element content using the
		/// ignorableWhitespace method rather than this one (validating parsers must
		/// do so).

	virtual void ignorableWhitespace(const XMLChar ch[], int start, int length) = 0;
		/// Receive notification of ignorable whitespace in element content.
		/// 
		/// Validating Parsers must use this method to report each chunk of whitespace
		/// in element content (see the W3C XML 1.0 recommendation, section 2.10): non-validating
		/// parsers may also use this method if they are capable of parsing and using
		/// content models.
		/// 
		/// SAX parsers may return all contiguous whitespace in a single chunk, or they
		/// may split it into several chunks; however, all of the characters in any
		/// single event must come from the same external entity, so that the Locator
		/// provides useful information.
		/// 
		/// The application must not attempt to read from the array outside of the specified
		/// range.

	virtual void processingInstruction(const XMLString& target, const XMLString& data) = 0;
		/// Receive notification of a processing instruction.
		/// 
		/// The Parser will invoke this method once for each processing instruction
		/// found: note that processing instructions may occur before or after the main
		/// document element.
		/// 
		/// A SAX parser must never report an XML declaration (XML 1.0, section 2.8)
		/// or a text declaration (XML 1.0, section 4.3.1) using this method.
		/// 
		/// Like characters(), processing instruction data may have characters that
		/// need more than one char value.

	virtual void startPrefixMapping(const XMLString& prefix, const XMLString& uri) = 0;
		/// Begin the scope of a prefix-URI Namespace mapping.
		/// 
		/// The information from this event is not necessary for normal Namespace processing:
		/// the SAX XML reader will automatically replace prefixes for element and attribute
		/// names when the http://xml.org/sax/features/namespaces feature is true (the
		/// default).
		/// 
		/// There are cases, however, when applications need to use prefixes in character
		/// data or in attribute values, where they cannot safely be expanded automatically;
		/// the start/endPrefixMapping event supplies the information to the application
		/// to expand prefixes in those contexts itself, if necessary.
		/// 
		/// Note that start/endPrefixMapping events are not guaranteed to be properly
		/// nested relative to each other: all startPrefixMapping events will occur
		/// immediately before the corresponding startElement event, and all endPrefixMapping
		/// events will occur immediately after the corresponding endElement event,
		/// but their order is not otherwise guaranteed.
		/// 
		/// There should never be start/endPrefixMapping events for the "xml" prefix,
		/// since it is predeclared and immutable.

	virtual void endPrefixMapping(const XMLString& prefix) = 0;
		/// End the scope of a prefix-URI mapping.
		/// 
		/// See startPrefixMapping for details. These events will always occur immediately
		/// after the corresponding endElement event, but the order of endPrefixMapping
		/// events is not otherwise guaranteed.

	virtual void skippedEntity(const XMLString& name) = 0;
		/// Receive notification of a skipped entity. This is not called for entity
		/// references within markup constructs such as element start tags or markup
		/// declarations. (The XML recommendation requires reporting skipped external
		/// entities. SAX also reports internal entity expansion/non-expansion, except
		/// within markup constructs.)
		/// 
		/// The Parser will invoke this method each time the entity is skipped. Non-validating
		/// processors may skip entities if they have not seen the declarations (because,
		/// for example, the entity was declared in an external DTD subset). All processors
		/// may skip external entities, depending on the values of the http://xml.org/sax/features/external-general-entities
		/// and the http://xml.org/sax/features/external-parameter-entities properties.

protected:
	virtual ~ContentHandler();
};


} } // namespace Poco::XML


#endif // SAX_ContentHandler_INCLUDED
