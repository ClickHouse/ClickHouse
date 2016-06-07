//
// XMLReader.h
//
// $Id: //poco/1.4/XML/include/Poco/SAX/XMLReader.h#1 $
//
// Library: XML
// Package: SAX
// Module:  SAX
//
// SAX2 XMLReader Interface.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef SAX_XMLReader_INCLUDED
#define SAX_XMLReader_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/XML/XMLString.h"


namespace Poco {
namespace XML {


class EntityResolver;
class DTDHandler;
class ContentHandler;
class ErrorHandler;
class InputSource;
class LexicalHandler;
class NamespaceHandler;


class XML_API XMLReader
	/// Interface for reading an XML document using callbacks. 
	/// XMLReader is the interface that an XML parser's SAX2 driver must 
	/// implement. This interface allows an application to set and
	/// query features and properties in the parser, to register event handlers 
	/// for document processing, and to initiate a document parse.
	/// All SAX interfaces are assumed to be synchronous: the parse methods must not 
	/// return until parsing is complete, and readers
	/// must wait for an event-handler callback to return before reporting the next event.
{
public:
	virtual void setEntityResolver(EntityResolver* pResolver) = 0;
		/// Allow an application to register an entity resolver.
		///
		/// If the application does not register an entity resolver, the 
		/// XMLReader will perform its own default resolution.
		/// 
		/// Applications may register a new or different resolver in the middle of a 
		/// parse, and the SAX parser must begin using the new resolver immediately.

	virtual EntityResolver* getEntityResolver() const = 0;
		/// Return the current entity resolver.

	virtual void setDTDHandler(DTDHandler* pDTDHandler) = 0;
		/// Allow an application to register a DTD event handler.
		///
		/// If the application does not register a DTD handler, all DTD events reported by 
		/// the SAX parser will be silently ignored.
		/// 
		/// Applications may register a new or different handler in the middle of a parse, 
		/// and the SAX parser must begin using the new handler immediately.

	virtual DTDHandler* getDTDHandler() const = 0;
		/// Return the current DTD handler.

	virtual void setContentHandler(ContentHandler* pContentHandler) = 0;
		/// Allow an application to register a content event handler.
		/// 
		/// If the application does not register a content handler, all content events 
		/// reported by the SAX parser will be silently ignored.
		/// 
		/// Applications may register a new or different handler in the middle of a parse, 
		/// and the SAX parser must begin using the new handler immediately.

	virtual ContentHandler* getContentHandler() const = 0;
		/// Return the current content handler.

	virtual void setErrorHandler(ErrorHandler* pErrorHandler) = 0;
		/// Allow an application to register an error event handler.
		///
		/// If the application does not register an error handler, all error events reported by 
		/// the SAX parser will be silently ignored; however, normal processing may not continue. 
		/// It is highly recommended that all SAX applications implement an error handler to avoid 
		/// unexpected bugs.
		/// 
		/// Applications may register a new or different handler in the middle of a parse, and the 
		/// SAX parser must begin using the new handler immediately.

	virtual ErrorHandler* getErrorHandler() const = 0;
		/// Return the current error handler.
	
	virtual void setFeature(const XMLString& featureId, bool state) = 0;
		/// Set the state of a feature.
		///
		/// The feature name is any fully-qualified URI. It is possible for an XMLReader to 
		/// expose a feature value but to be unable to change the current value. Some feature 
		/// values may be immutable or mutable only in specific contexts, such as before, during, 
		/// or after a parse.
		/// 
		/// All XMLReaders are required to support setting http://xml.org/sax/features/namespaces 
		/// to true and http://xml.org/sax/features/namespace-prefixes to false.

	virtual bool getFeature(const XMLString& featureId) const = 0;
		/// Look up the value of a feature.
		///
		/// The feature name is any fully-qualified URI. It is possible for an XMLReader 
		/// to recognize a feature name but temporarily be unable to return its value. 
		/// Some feature values may be available only in specific contexts, such as before, 
		/// during, or after a parse. Also, some feature values may not be programmatically 
		/// accessible. (In the case of an adapter for SAX1 Parser, there is no 
		/// implementation-independent way to expose whether the underlying parser is performing 
		/// validation, expanding external entities, and so forth.)
		/// 
		/// All XMLReaders are required to recognize the 
		/// http://xml.org/sax/features/namespaces and the 
		/// http://xml.org/sax/features/namespace-prefixes feature names.
		/// Implementors are free (and encouraged) to invent their own features, 
		/// using names built on their own URIs.
		
	virtual void setProperty(const XMLString& propertyId, const XMLString& value) = 0;
		/// Set the value of a property.
		/// 
		/// The property name is any fully-qualified URI. It is possible for an XMLReader 
		/// to recognize a property name but to be unable to change the current value. 
		/// Some property values may be immutable or mutable only in specific contexts, 
		/// such as before, during, or after a parse.
		/// 
		/// XMLReaders are not required to recognize setting any specific property names, though a 
		/// core set is defined by SAX2.
		/// 
		/// This method is also the standard mechanism for setting extended handlers.

	virtual void setProperty(const XMLString& propertyId, void* value) = 0;
		/// Set the value of a property.
		/// See also setProperty(const XMLString&, const XMLString&).

	virtual void* getProperty(const XMLString& propertyId) const = 0;
		/// Look up the value of a property.
		/// String values are returned as XMLChar*
		/// The property name is any fully-qualified URI. It is possible for an XMLReader to 
		/// recognize a property name but temporarily be unable to return its value. Some property 
		/// values may be available only in specific contexts, such as before, during, or after a parse.
		///
		/// XMLReaders are not required to recognize any specific property names, though an initial 
		/// core set is documented for SAX2.
		/// 
		/// Implementors are free (and encouraged) to invent their own properties, using names 
		/// built on their own URIs.

	virtual void parse(InputSource* pSource) = 0;
		/// Parse an XML document.
		/// 
		/// The application can use this method to instruct the XML reader to begin parsing an 
		/// XML document from any valid input source (a character stream, a byte stream, or a URI).
		/// 
		/// Applications may not invoke this method while a parse is in progress (they should create 
		/// a new XMLReader instead for each nested XML document). Once a parse is complete, an 
		/// application may reuse the same XMLReader object, possibly with a different input source. 
		/// Configuration of the XMLReader object (such as handler bindings and values established for 
		/// feature flags and properties) is unchanged by completion of a parse, unless the definition of that 
		/// aspect of the configuration explicitly specifies other behavior. (For example, feature flags or 
		/// properties exposing characteristics of the document being parsed.)
		/// 
		/// During the parse, the XMLReader will provide information about the XML document through the registered 
		/// event handlers.
		///
		/// This method is synchronous: it will not return until parsing has ended. If a client application 
		/// wants to terminate parsing early, it should throw an exception.

	virtual void parse(const XMLString& systemId) = 0;
		/// Parse an XML document from a system identifier.
		/// See also parse(InputSource*).
		
	virtual void parseMemoryNP(const char* xml, std::size_t size) = 0;
		/// Parse an XML document from memory.
		/// See also parse(InputSource*).

	// SAX Features
	static const XMLString FEATURE_VALIDATION;
	static const XMLString FEATURE_NAMESPACES;
	static const XMLString FEATURE_NAMESPACE_PREFIXES;
	static const XMLString FEATURE_EXTERNAL_GENERAL_ENTITIES;
	static const XMLString FEATURE_EXTERNAL_PARAMETER_ENTITIES;
	static const XMLString FEATURE_STRING_INTERNING;
	
	// SAX Properties
	static const XMLString PROPERTY_DECLARATION_HANDLER;
	static const XMLString PROPERTY_LEXICAL_HANDLER;

protected:
	virtual ~XMLReader();
};


} } // namespace Poco::XML


#endif // SAX_XMLReader_INCLUDED
