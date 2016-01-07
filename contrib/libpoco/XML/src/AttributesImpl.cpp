//
// AttributesImpl.cpp
//
// $Id: //poco/1.4/XML/src/AttributesImpl.cpp#1 $
//
// Library: XML
// Package: SAX
// Module:  SAX
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/SAX/AttributesImpl.h"


namespace Poco {
namespace XML {


AttributesImpl::AttributesImpl()
{
	_empty.specified = false;
	_empty.type = XML_LIT("CDATA");
}


AttributesImpl::AttributesImpl(const Attributes& attributes)
{
	_empty.specified = false;
	_empty.type = XML_LIT("CDATA");
	setAttributes(attributes);
}


AttributesImpl::AttributesImpl(const AttributesImpl& attributes):
	_attributes(attributes._attributes),
	_empty(attributes._empty)
{
}


AttributesImpl::~AttributesImpl()
{
}


AttributesImpl& AttributesImpl::operator = (const AttributesImpl& attributes)
{
	if (&attributes != this)
	{
		_attributes = attributes._attributes;
	}
	return *this;
}


int AttributesImpl::getIndex(const XMLString& qname) const
{
	int i = 0;
	AttributeVec::const_iterator it;
	for (it = _attributes.begin(); it != _attributes.end(); ++it)
	{
		if (it->qname == qname) return i;
		++i;
	}
	return -1;
}


int AttributesImpl::getIndex(const XMLString& namespaceURI, const XMLString& localName) const
{
	int i = 0;
	AttributeVec::const_iterator it;
	for (it = _attributes.begin(); it != _attributes.end(); ++it)
	{
		if (it->namespaceURI == namespaceURI && it->localName == localName) return i;
		++i;
	}
	return -1;
}


void AttributesImpl::setValue(int i, const XMLString& value)
{
	poco_assert (0 <= i && i < static_cast<int>(_attributes.size()));
	_attributes[i].value     = value;
	_attributes[i].specified = true;
}


void AttributesImpl::setValue(const XMLString& qname, const XMLString& value)
{
	Attribute* pAttr = find(qname);
	if (pAttr)
	{
		pAttr->value     = value;
		pAttr->specified = true;
	}
}


void AttributesImpl::setValue(const XMLString& namespaceURI, const XMLString& localName, const XMLString& value)
{
	Attribute* pAttr = find(namespaceURI, localName);
	if (pAttr)
	{
		pAttr->value     = value;
		pAttr->specified = true;
	}
}


void AttributesImpl::setAttributes(const Attributes& attributes)
{
	if (&attributes != this)
	{
		int count = attributes.getLength();
		_attributes.clear();
		_attributes.reserve(count);
		for (int i = 0; i < count; i++)
		{
			addAttribute(attributes.getURI(i), attributes.getLocalName(i), attributes.getQName(i), attributes.getType(i), attributes.getValue(i));
		}
	}
}


void AttributesImpl::setAttribute(int i, const XMLString& namespaceURI, const XMLString& localName, const XMLString& qname, const XMLString& type, const XMLString& value)
{
	poco_assert (0 <= i && i < static_cast<int>(_attributes.size()));
	_attributes[i].namespaceURI = namespaceURI;
	_attributes[i].localName    = localName;
	_attributes[i].qname        = qname;
	_attributes[i].type         = type;
	_attributes[i].value        = value;
	_attributes[i].specified    = true;
}


void AttributesImpl::addAttribute(const XMLString& namespaceURI, const XMLString& localName, const XMLString& qname, const XMLString& type, const XMLString& value)
{
	AttributeVec::iterator it = _attributes.insert(_attributes.end(), Attribute());
	it->namespaceURI = namespaceURI;
	it->localName    = localName;
	it->qname        = qname;
	it->value        = value;
	it->type         = type;
	it->specified    = true;
}


void AttributesImpl::addAttribute(const XMLString& namespaceURI, const XMLString& localName, const XMLString& qname, const XMLString& type, const XMLString& value, bool specified)
{
	AttributeVec::iterator it = _attributes.insert(_attributes.end(), Attribute());
	it->namespaceURI = namespaceURI;
	it->localName    = localName;
	it->qname        = qname;
	it->value        = value;
	it->type         = type;
	it->specified    = specified;
}


void AttributesImpl::addAttribute(const XMLChar* namespaceURI, const XMLChar* localName, const XMLChar* qname, const XMLChar* type, const XMLChar* value, bool specified)
{
	AttributeVec::iterator it = _attributes.insert(_attributes.end(), Attribute());
	it->namespaceURI = namespaceURI;
	it->localName    = localName;
	it->qname        = qname;
	it->value        = value;
	it->type         = type;
	it->specified    = specified;
}


void AttributesImpl::removeAttribute(int i)
{
	int cur = 0;
	for (AttributeVec::iterator it = _attributes.begin(); it != _attributes.end(); ++it, ++cur)
	{
		if (cur == i)
		{
			_attributes.erase(it);
			break;
		}
	}
}


void AttributesImpl::removeAttribute(const XMLString& qname)
{
	for (AttributeVec::iterator it = _attributes.begin(); it != _attributes.end(); ++it)
	{
		if (it->qname == qname)
		{
			_attributes.erase(it);
			break;
		}
	}
}


void AttributesImpl::removeAttribute(const XMLString& namespaceURI, const XMLString& localName)
{
	for (AttributeVec::iterator it = _attributes.begin(); it != _attributes.end(); ++it)
	{
		if (it->namespaceURI == namespaceURI && it->localName == localName)
		{
			_attributes.erase(it);
			break;
		}
	}
}


void AttributesImpl::clear()
{
	_attributes.clear();
}


void AttributesImpl::reserve(std::size_t capacity)
{
	_attributes.reserve(capacity);
}


void AttributesImpl::setLocalName(int i, const XMLString& localName)
{
	poco_assert (0 <= i && i < static_cast<int>(_attributes.size()));
	_attributes[i].localName = localName;
}


void AttributesImpl::setQName(int i, const XMLString& qname)
{
	poco_assert (0 <= i && i < static_cast<int>(_attributes.size()));
	_attributes[i].qname = qname;
}


void AttributesImpl::setType(int i, const XMLString& type)
{
	poco_assert (0 <= i && i < static_cast<int>(_attributes.size()));
	_attributes[i].type = type;
}


void AttributesImpl::setURI(int i, const XMLString& namespaceURI)
{
	poco_assert (0 <= i && i < static_cast<int>(_attributes.size()));
	_attributes[i].namespaceURI = namespaceURI;
}


AttributesImpl::Attribute* AttributesImpl::find(const XMLString& qname) const
{
	for (AttributeVec::const_iterator it = _attributes.begin(); it != _attributes.end(); ++it)
	{
		if (it->qname == qname) 
			return const_cast<Attribute*>(&(*it));
	}
	return 0;
}


AttributesImpl::Attribute* AttributesImpl::find(const XMLString& namespaceURI, const XMLString& localName) const
{
	for (AttributeVec::const_iterator it = _attributes.begin(); it != _attributes.end(); ++it)
	{
		if (it->namespaceURI == namespaceURI && it->localName == localName) 
			return const_cast<Attribute*>(&(*it));
	}
	return 0;
}


} } // namespace Poco::XML
