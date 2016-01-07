//
// AttributesImpl.h
//
// $Id: //poco/1.4/XML/include/Poco/SAX/AttributesImpl.h#1 $
//
// Library: XML
// Package: SAX
// Module:  SAX
//
// Implementation of the SAX2 Attributes Interface.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef SAX_AttributesImpl_INCLUDED
#define SAX_AttributesImpl_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/SAX/Attributes.h"
#include <vector>


namespace Poco {
namespace XML {


class XML_API AttributesImpl: public Attributes
	/// This class provides a default implementation of the SAX2 Attributes interface, 
	/// with the addition of manipulators so that the list can be modified or reused.
	/// 
	/// There are two typical uses of this class:
	///     1. to take a persistent snapshot of an Attributes object in a startElement event; or
	///     2. to construct or modify an Attributes object in a SAX2 driver or filter.
{
public:
	struct Attribute
	{
		XMLString localName;
		XMLString namespaceURI;
		XMLString qname;
		XMLString value;
		XMLString type;
		bool      specified;
	};
	typedef std::vector<Attribute> AttributeVec;
	typedef AttributeVec::const_iterator iterator;

	AttributesImpl();
		/// Creates the AttributesImpl.
		
	AttributesImpl(const Attributes& attributes);
		/// Creates the AttributesImpl by copying another one.

	AttributesImpl(const AttributesImpl& attributes);
		/// Creates the AttributesImpl by copying another one.

	~AttributesImpl();
		/// Destroys the AttributesImpl.

	AttributesImpl& operator = (const AttributesImpl& attributes);
		/// Assignment operator.

	int getIndex(const XMLString& name) const;
	int getIndex(const XMLString& namespaceURI, const XMLString& localName) const;
	int getLength() const;
	const XMLString& getLocalName(int i) const;
	const XMLString& getQName(int i) const;
	const XMLString& getType(int i) const;
	const XMLString& getType(const XMLString& qname) const;
	const XMLString& getType(const XMLString& namespaceURI, const XMLString& localName) const;
	const XMLString& getValue(int i) const;
	const XMLString& getValue(const XMLString& qname) const;
	const XMLString& getValue(const XMLString& namespaceURI, const XMLString& localName) const;
	const XMLString& getURI(int i) const;

	bool isSpecified(int i) const;
		/// Returns true unless the attribute value was provided by DTD defaulting.
		/// Extension from Attributes2 interface.

	bool isSpecified(const XMLString& qname) const;
		/// Returns true unless the attribute value was provided by DTD defaulting.
		/// Extension from Attributes2 interface.

	bool isSpecified(const XMLString& namespaceURI, const XMLString& localName) const;
		/// Returns true unless the attribute value was provided by DTD defaulting.
		/// Extension from Attributes2 interface.

	void setValue(int i, const XMLString& value);
		/// Sets the value of an attribute.

	void setValue(const XMLString& qname, const XMLString& value);
		/// Sets the value of an attribute.

	void setValue(const XMLString& namespaceURI, const XMLString& localName, const XMLString& value);
		/// Sets the value of an attribute.

	void setAttributes(const Attributes& attributes);
		/// Copies the attributes from another Attributes object.

	void setAttribute(int i, const XMLString& namespaceURI, const XMLString& localName, const XMLString& qname, const XMLString& type, const XMLString& value);
		/// Sets an attribute.

	void addAttribute(const XMLString& namespaceURI, const XMLString& localName, const XMLString& qname, const XMLString& type, const XMLString& value);
		/// Adds an attribute to the end of the list.

	void addAttribute(const XMLString& namespaceURI, const XMLString& localName, const XMLString& qname, const XMLString& type, const XMLString& value, bool specified);
		/// Adds an attribute to the end of the list.

	void addAttribute(const XMLChar* namespaceURI, const XMLChar* localName, const XMLChar* qname, const XMLChar* type, const XMLChar* value, bool specified);
		/// Adds an attribute to the end of the list.

	Attribute& addAttribute();
		/// Add an (empty) attribute to the end of the list.
		/// For internal use only.
		/// The returned Attribute element must be filled by the caller.

	void removeAttribute(int i);
		/// Removes an attribute.

	void removeAttribute(const XMLString& qname);
		/// Removes an attribute.

	void removeAttribute(const XMLString& namespaceURI, const XMLString& localName);
		/// Removes an attribute.

	void clear();
		/// Removes all attributes.
		
	void reserve(std::size_t capacity);
		/// Reserves capacity in the internal vector.

	void setLocalName(int i, const XMLString& localName);
		/// Sets the local name of an attribute.

	void setQName(int i, const XMLString& qname);
		/// Sets the qualified name of an attribute.

	void setType(int i, const XMLString& type);
		/// Sets the type of an attribute.

	void setURI(int i, const XMLString& namespaceURI);
		/// Sets the namespace URI of an attribute.

	iterator begin() const;
		/// Iterator support.
		
	iterator end() const;
		/// Iterator support.

protected:	
	Attribute* find(const XMLString& qname) const;
	Attribute* find(const XMLString& namespaceURI, const XMLString& localName) const;

private:
	AttributeVec _attributes;
	Attribute _empty;
};


//
// inlines
//
inline AttributesImpl::iterator AttributesImpl::begin() const
{
	return _attributes.begin();
}


inline AttributesImpl::iterator AttributesImpl::end() const
{
	return _attributes.end();
}


inline AttributesImpl::Attribute& AttributesImpl::addAttribute()
{
	_attributes.push_back(_empty);
	return _attributes.back();
}


inline int AttributesImpl::getLength() const
{
	return (int) _attributes.size();
}


inline const XMLString& AttributesImpl::getLocalName(int i) const
{
	poco_assert (0 <= i && i < static_cast<int>(_attributes.size()));
	return _attributes[i].localName;
}


inline const XMLString& AttributesImpl::getQName(int i) const
{
	poco_assert (0 <= i && i < static_cast<int>(_attributes.size()));
	return _attributes[i].qname;
}


inline const XMLString& AttributesImpl::getType(int i) const
{
	poco_assert (0 <= i && i < static_cast<int>(_attributes.size()));
	return _attributes[i].type;
}


inline const XMLString& AttributesImpl::getType(const XMLString& qname) const
{
	Attribute* pAttr = find(qname);
	if (pAttr)
		return pAttr->type;
	else
		return _empty.type;
}


inline const XMLString& AttributesImpl::getType(const XMLString& namespaceURI, const XMLString& localName) const
{
	Attribute* pAttr = find(namespaceURI, localName);
	if (pAttr)
		return pAttr->type;
	else
		return _empty.type;
}


inline const XMLString& AttributesImpl::getValue(int i) const
{
	poco_assert (0 <= i && i < static_cast<int>(_attributes.size()));
	return _attributes[i].value;
}


inline const XMLString& AttributesImpl::getValue(const XMLString& qname) const
{
	Attribute* pAttr = find(qname);
	if (pAttr)
		return pAttr->value;
	else
		return _empty.value;
}


inline const XMLString& AttributesImpl::getValue(const XMLString& namespaceURI, const XMLString& localName) const
{
	Attribute* pAttr = find(namespaceURI, localName);
	if (pAttr)
		return pAttr->value;
	else
		return _empty.value;
}


inline const XMLString& AttributesImpl::getURI(int i) const
{
	poco_assert (0 <= i && i < static_cast<int>(_attributes.size()));
	return _attributes[i].namespaceURI;
}


inline bool AttributesImpl::isSpecified(int i) const
{
	poco_assert (0 <= i && i < static_cast<int>(_attributes.size()));
	return _attributes[i].specified;
}


inline bool AttributesImpl::isSpecified(const XMLString& qname) const
{
	Attribute* pAttr = find(qname);
	if (pAttr)
		return pAttr->specified;
	else
		return false;
}


inline bool AttributesImpl::isSpecified(const XMLString& namespaceURI, const XMLString& localName) const
{
	Attribute* pAttr = find(namespaceURI, localName);
	if (pAttr)
		return pAttr->specified;
	else
		return false;
}


} } // namespace Poco::XML


#endif // SAX_AttributesImpl_INCLUDED
