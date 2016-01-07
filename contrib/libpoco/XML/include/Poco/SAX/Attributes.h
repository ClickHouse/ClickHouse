//
// Attributes.h
//
// $Id: //poco/1.4/XML/include/Poco/SAX/Attributes.h#1 $
//
// Library: XML
// Package: SAX
// Module:  SAX
//
// SAX2 Attributes Interface.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef SAX_Attributes_INCLUDED
#define SAX_Attributes_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/XML/XMLString.h"


namespace Poco {
namespace XML {


class XML_API Attributes
	/// Interface for a list of XML attributes.
	/// This interface allows access to a list of attributes in three different ways:
	///   1.by attribute index; 
	///   2.by Namespace-qualified name; or 
	///   3.by qualified (prefixed) name. 
	/// 
	/// The list will not contain attributes that were declared #IMPLIED but not 
	/// specified in the start tag. It will also not contain
	/// attributes used as Namespace declarations (xmlns*) unless the 
	/// http://xml.org/sax/features/namespace-prefixes
	/// feature is set to true (it is false by default).
	/// 
	/// If the namespace-prefixes feature (see above) is false, access by 
	/// qualified name may not be available; if the
	/// http://xml.org/sax/features/namespaces feature is false, access by 
	/// Namespace-qualified names may not be available.
	/// This interface replaces the now-deprecated SAX1 AttributeList interface, 
	/// which does not contain Namespace support. In
	/// addition to Namespace support, it adds the getIndex methods (below).
	/// The order of attributes in the list is unspecified, and will vary from 
	/// implementation to implementation.
{
public:
	virtual int getIndex(const XMLString& name) const = 0;
		/// Look up the index of an attribute by a qualified name.

	virtual int getIndex(const XMLString& namespaceURI, const XMLString& localName) const = 0;
		/// Look up the index of an attribute by a namspace name.

	virtual int getLength() const = 0;
		/// Return the number of attributes in the list.
		///
		/// Once you know the number of attributes, you can iterate through the list.
		
	virtual const XMLString& getLocalName(int i) const = 0;
		/// Look up a local attribute name by index.

	virtual const XMLString& getQName(int i) const = 0;
		/// Look up a qualified attribute name by index.

	virtual const XMLString& getType(int i) const = 0;
		/// Look up an attribute type by index.
		///
		/// The attribute type is one of the strings "CDATA", "ID", "IDREF", "IDREFS", "NMTOKEN", 
		/// "NMTOKENS", "ENTITY", "ENTITIES", or "NOTATION" (always in upper case).
		///
		/// If the parser has not read a declaration for the attribute, or if the parser does not 
		/// report attribute types, then it must return the value "CDATA" as stated in the XML 1.0 
		/// Recommendation (clause 3.3.3, "Attribute-Value Normalization").
		/// 
		/// For an enumerated attribute that is not a notation, the parser will report the type 
		/// as "NMTOKEN".

	virtual const XMLString& getType(const XMLString& qname) const = 0;
		/// Look up an attribute type by a qualified name.
		///
		/// See getType(int) for a description of the possible types.

	virtual const XMLString& getType(const XMLString& namespaceURI, const XMLString& localName) const = 0;
		/// Look up an attribute type by a namespace name.
		///
		/// See getType(int) for a description of the possible types.

	virtual const XMLString& getValue(int i) const = 0;
		/// Look up an attribute value by index.
		///
		/// If the attribute value is a list of tokens (IDREFS, ENTITIES, or NMTOKENS), the tokens 
		/// will be concatenated into a single string with each token separated by a single space.

	virtual const XMLString& getValue(const XMLString& qname) const = 0;
		/// Look up an attribute value by a qualified name.
		///
		/// See getValue(int) for a description of the possible values.

	virtual const XMLString& getValue(const XMLString& uri, const XMLString& localName) const = 0;
		/// Look up an attribute value by a namespace name.
		///
		/// See getValue(int) for a description of the possible values.

	virtual const XMLString& getURI(int i) const = 0;
		/// Look up a namespace URI by index.

protected:
	virtual ~Attributes();
};


} } // namespace Poco::XML


#endif // SAX_Attributes_INCLUDED
