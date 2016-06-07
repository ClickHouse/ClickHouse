//
// Name.cpp
//
// $Id: //poco/1.4/XML/src/Name.cpp#1 $
//
// Library: XML
// Package: XML
// Module:  Name
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/XML/Name.h"
#include <algorithm>


namespace Poco {
namespace XML {


const XMLString Name::EMPTY_NAME;


Name::Name()
{
}


Name::Name(const XMLString& qname):
	_qname(qname)
{
}


Name::Name(const XMLString& qname, const XMLString& namespaceURI):
	_qname(qname),
	_namespaceURI(namespaceURI),
	_localName(localName(qname))
{
}


Name::Name(const XMLString& qname, const XMLString& namespaceURI, const XMLString& localName):
	_qname(qname),
	_namespaceURI(namespaceURI),
	_localName(localName)
{
}


Name::Name(const Name& name):
	_qname(name._qname),
	_namespaceURI(name._namespaceURI),
	_localName(name._localName)
{
}

	
Name::~Name()
{
}


Name& Name::operator = (const Name& name)
{
	if (this != &name)
	{
		_qname        = name._qname;
		_namespaceURI = name._namespaceURI;
		_localName    = name._localName;
	}
	return *this;
}


void Name::swap(Name& name)
{
	std::swap(_qname, name._qname);
	std::swap(_namespaceURI, name._namespaceURI);
	std::swap(_localName, name._localName);
}


void Name::assign(const XMLString& qname)
{
	_qname        = qname;
	_namespaceURI.clear();
	_localName.clear();
}


void Name::assign(const XMLString& qname, const XMLString& namespaceURI)
{
	_qname        = qname;
	_namespaceURI = namespaceURI;
	_localName    = localName(qname);
}


void Name::assign(const XMLString& qname, const XMLString& namespaceURI, const XMLString& localName)
{
	_qname        = qname;
	_namespaceURI = namespaceURI;
	_localName    = localName;
}


bool Name::equals(const Name& name) const
{
	return name._namespaceURI == _namespaceURI && name._localName == _localName && name._qname == _qname;
}


bool Name::equals(const XMLString& qname, const XMLString& namespaceURI, const XMLString& localName) const
{
	return _namespaceURI == namespaceURI && _localName == localName && _qname == qname;
}


bool Name::equalsWeakly(const XMLString& qname, const XMLString& namespaceURI, const XMLString& localName) const
{
	return (_qname == qname && !qname.empty()) || (_namespaceURI == namespaceURI && _localName == localName && !_localName.empty());
}


XMLString Name::prefix() const
{
	return prefix(_qname);
}


void Name::split(const XMLString& qname, XMLString& prefix, XMLString& localName)
{
	XMLString::size_type pos = qname.find(':');
	if (pos != XMLString::npos)
	{
		prefix.assign(qname, 0, pos);
		localName.assign(qname, pos + 1, qname.size() - pos - 1);
	}
	else
	{
		prefix.clear();
		localName.assign(qname);
	}
}


XMLString Name::localName(const XMLString& qname)
{
	XMLString::size_type pos = qname.find(':');
	if (pos != XMLString::npos) 
		return XMLString(qname, pos + 1, qname.size() - pos - 1);
	else
		return qname;
}


XMLString Name::prefix(const XMLString& qname)
{
	XMLString::size_type pos = qname.find(':');
	if (pos != XMLString::npos)
		return XMLString(qname, 0, pos);
	else
		return EMPTY_NAME;
}


} } // namespace Poco::XML
