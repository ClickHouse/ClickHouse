//
// DeclHandler.h
//
// $Id: //poco/1.4/XML/include/Poco/SAX/DeclHandler.h#1 $
//
// Library: XML
// Package: SAX
// Module:  SAX
//
// SAX2-ext DeclHandler Interface.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef SAX_DeclHandler_INCLUDED
#define SAX_DeclHandler_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/XML/XMLString.h"


namespace Poco {
namespace XML {


class XML_API DeclHandler
	/// This is an optional extension handler for SAX2 to provide information 
	/// about DTD declarations in an XML document. XML
	/// readers are not required to support this handler, and this handler is 
	/// not included in the core SAX2 distribution.
	///
	/// Note that data-related DTD declarations (unparsed entities and notations) 
	/// are already reported through the DTDHandler interface.
	/// If you are using the declaration handler together with a lexical handler, 
	/// all of the events will occur between the startDTD and the endDTD events.
	/// To set the DeclHandler for an XML reader, use the setProperty method 
	/// with the propertyId "http://xml.org/sax/properties/declaration-handler". 
	/// If the reader does not support declaration events, it will throw a
	/// SAXNotRecognizedException or a SAXNotSupportedException when you attempt to 
	/// register the handler.
{
public:
	virtual void attributeDecl(const XMLString& eName, const XMLString& aName, const XMLString* valueDefault, const XMLString* value) = 0;
		/// Report an attribute type declaration.
		/// 
		/// Only the effective (first) declaration for an attribute will be reported. 
		/// The type will be one of the strings "CDATA", "ID", "IDREF", "IDREFS", 
		/// "NMTOKEN", "NMTOKENS", "ENTITY", "ENTITIES", a parenthesized token group 
		/// with the separator "|" and all whitespace removed, or the word "NOTATION" 
		/// followed by a space followed by a parenthesized token group with all whitespace 
		/// removed.
		///
		/// The value will be the value as reported to applications, appropriately normalized 
		/// and with entity and character references expanded. 
		
	virtual void elementDecl(const XMLString& name, const XMLString& model) = 0;
		/// Report an element type declaration.
		///
		/// The content model will consist of the string "EMPTY", the string "ANY", or a 
		/// parenthesised group, optionally followed by an occurrence indicator. The model 
		/// will be normalized so that all parameter entities are fully resolved and all 
		/// whitespace is removed,and will include the enclosing parentheses. Other 
		/// normalization (such as removing redundant parentheses or simplifying occurrence 
		/// indicators) is at the discretion of the parser.
		
	virtual void externalEntityDecl(const XMLString& name, const XMLString* publicId, const XMLString& systemId) = 0;
		/// Report an external entity declaration.
		/// 
		/// Only the effective (first) declaration for each entity will be reported.
		/// 
		/// If the system identifier is a URL, the parser must resolve it fully before 
		/// passing it to the application.

	virtual void internalEntityDecl(const XMLString& name, const XMLString& value) = 0;
		/// Report an internal entity declaration.
		///
		/// Only the effective (first) declaration for each entity will be reported. All 
		/// parameter entities in the value will be expanded, but general entities will not.

protected:
	virtual ~DeclHandler();
};


} } // namespace Poco::XML


#endif // SAX_DeclHandler_INCLUDED
