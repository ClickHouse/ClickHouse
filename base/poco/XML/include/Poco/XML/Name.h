//
// Name.h
//
// Library: XML
// Package: XML
// Module:  Name
//
// Definition of the Name class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef XML_Name_INCLUDED
#define XML_Name_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/XML/XMLString.h"


namespace Poco {
namespace XML {


class XML_API Name
	/// An XML element or attribute name, consisting of a
	/// qualified name, a namespace URI and a local name.
{
public:
	Name();
		/// Creates an empty Name.
		
	Name(const XMLString& qname);
		/// Creates a Name from a qualified name only.

	Name(const XMLString& qname, const XMLString& namespaceURI);
		/// Creates a Name from a qualified name and a namespace URI.
		/// The local name is extracted from the qualified name.
		
	Name(const XMLString& qname, const XMLString& namespaceURI, const XMLString& localName);
		/// Creates a Name from a qualified name, a namespace URI and a local name.
		
	Name(const Name& name);
		/// Copy constructor.

	Name(Name&& name) noexcept;
		/// Move constructor.
		
	~Name();
		/// Destroys the name.
		
	Name& operator = (const Name& name);
		/// Assignment operator.

	Name& operator = (Name&& name) noexcept;
		/// Move assignment.
		
	void swap(Name& name);
		/// Swaps the name with another one.	
		
	void assign(const XMLString& qname);
		/// Assigns a new value to the name.

	void assign(const XMLString& qname, const XMLString& namespaceURI);
		/// Assigns new values to the name.
		/// The local name is extracted from the qualified name.

	void assign(const XMLString& qname, const XMLString& namespaceURI, const XMLString& localName);
		/// Assigns new values to the name.
	
	bool equals(const Name& name) const;
		/// Returns true if both names are equal.
		
	bool equals(const XMLString& qname, const XMLString& namespaceURI, const XMLString& localName) const;
		/// Returns true if all the name's components are equal to the given ones.

	bool equalsWeakly(const XMLString& qname, const XMLString& namespaceURI, const XMLString& localName) const;
		/// Returns true if either the qnames are identical or the namespaceURIs and the localNames are identical.
	
	const XMLString& qname() const;
		/// Returns the qualified name.
		
	const XMLString& namespaceURI() const;
		/// Returns the namespace URI.
		
	const XMLString& localName() const;
		/// Returns the local name.
		
	XMLString prefix() const;
		/// Returns the namespace prefix.
		
	static void split(const XMLString& qname, XMLString& prefix, XMLString& localName);
		/// Splits the given qualified name into its prefix and localName parts.
	
	static XMLString localName(const XMLString& qname);
		/// Returns the local name part of the given qualified name.

	static XMLString prefix(const XMLString& qname);
		/// Returns the prefix part of the given qualified name.

	static const XMLString EMPTY_NAME;

private:
	XMLString _qname;
	XMLString _namespaceURI;
	XMLString _localName;
};


//
// inlines
//
inline const XMLString& Name::qname() const
{
	return _qname;
}


inline const XMLString& Name::namespaceURI() const
{
	return _namespaceURI;
}


inline const XMLString& Name::localName() const
{
	return _localName;
}


inline void swap(Name& n1, Name& n2)
{
	n1.swap(n2);
}


} } // namespace Poco::XML


#endif // XML_Name_INCLUDED
