//
// NamespaceSupport.cpp
//
// $Id: //poco/1.4/XML/src/NamespaceSupport.cpp#1 $
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


#include "Poco/SAX/NamespaceSupport.h"
#include "Poco/XML/Name.h"


namespace Poco {
namespace XML {


const XMLString NamespaceSupport::EMPTY_STRING;
const XMLString NamespaceSupport::XML_NAMESPACE          = toXMLString("http://www.w3.org/XML/1998/namespace");
const XMLString NamespaceSupport::XML_NAMESPACE_PREFIX   = toXMLString("xml");
const XMLString NamespaceSupport::XMLNS_NAMESPACE        = toXMLString("http://www.w3.org/xmlns/2000/");
const XMLString NamespaceSupport::XMLNS_NAMESPACE_PREFIX = toXMLString("xmlns");


NamespaceSupport::NamespaceSupport()
{
	reset();
}


NamespaceSupport::~NamespaceSupport()
{
}


bool NamespaceSupport::declarePrefix(const XMLString& prefix, const XMLString& namespaceURI)
{
	poco_assert (_contexts.size() > 0);

	Context& ctx = _contexts.back();
	if (ctx.find(prefix) == ctx.end())
	{
		ctx.insert(Context::value_type(prefix, namespaceURI));
		return true;
	}
	else return false;
}


bool NamespaceSupport::undeclarePrefix(const XMLString& prefix)
{
	for (ContextVec::reverse_iterator rit = _contexts.rbegin(); rit != _contexts.rend(); ++rit)
	{
		Context::iterator it = rit->find(prefix);
		if (it != rit->end())
		{
			rit->erase(it);
			return true;
		}
	}
	return false;
}


void NamespaceSupport::getDeclaredPrefixes(PrefixSet& prefixes) const
{
	prefixes.clear();
	const Context& ctx = _contexts.back();
	for (Context::const_iterator it = ctx.begin(); it != ctx.end(); ++it)
		prefixes.insert(it->first);
}


const XMLString& NamespaceSupport::getPrefix(const XMLString& namespaceURI) const
{
	for (ContextVec::const_reverse_iterator rit = _contexts.rbegin(); rit != _contexts.rend(); ++rit)
	{
		for (Context::const_iterator it = rit->begin(); it != rit->end(); ++it)
		{
			if (it->second == namespaceURI) 
				return it->first;
		}
	}
	return EMPTY_STRING;
}


bool NamespaceSupport::isMapped(const XMLString& namespaceURI) const
{
	for (ContextVec::const_reverse_iterator rit = _contexts.rbegin(); rit != _contexts.rend(); ++rit)
	{
		for (Context::const_iterator it = rit->begin(); it != rit->end(); ++it)
		{
			if (it->second == namespaceURI) 
				return true;
		}
	}
	return false;
}


void NamespaceSupport::getPrefixes(PrefixSet& prefixes) const
{
	prefixes.clear();
	for (ContextVec::const_reverse_iterator rit = _contexts.rbegin(); rit != _contexts.rend(); ++rit)
	{
		for (Context::const_iterator it = rit->begin(); it != rit->end(); ++it)
		{
			const XMLString& prefix = it->first;
			if (!prefix.empty() && prefixes.find(prefix) == prefixes.end()) 
				prefixes.insert(it->first);
		}
	}
}


void NamespaceSupport::getPrefixes(const XMLString& namespaceURI, PrefixSet& prefixes) const
{
	prefixes.clear();
	for (ContextVec::const_reverse_iterator rit = _contexts.rbegin(); rit != _contexts.rend(); ++rit)
	{
		for (Context::const_iterator it = rit->begin(); it != rit->end(); ++it)
		{
			const XMLString& prefix = it->first;
			if (it->second == namespaceURI && !prefix.empty() && prefixes.find(prefix) == prefixes.end()) 
				prefixes.insert(it->first);
		}
	}
}


const XMLString& NamespaceSupport::getURI(const XMLString& prefix) const
{
	for (ContextVec::const_reverse_iterator rit = _contexts.rbegin(); rit != _contexts.rend(); ++rit)
	{
		Context::const_iterator it = rit->find(prefix);
		if (it != rit->end()) 
			return it->second;
	}
	return EMPTY_STRING;
}


void NamespaceSupport::pushContext()
{
	_contexts.push_back(Context());
}


void NamespaceSupport::popContext()
{
	_contexts.pop_back();
}


bool NamespaceSupport::processName(const XMLString& qname, XMLString& namespaceURI, XMLString& localName, bool isAttribute) const
{
	XMLString prefix;
	Name::split(qname, prefix, localName);
	if (prefix.empty() && isAttribute)
	{
		namespaceURI.clear();
		return true;
	}
	else
	{
		namespaceURI = getURI(prefix);
		return !namespaceURI.empty() || prefix.empty();
	}
}


void NamespaceSupport::reset()
{
	_contexts.clear();
	pushContext();
	declarePrefix(XML_NAMESPACE_PREFIX, XML_NAMESPACE);
	declarePrefix(XMLNS_NAMESPACE_PREFIX, XMLNS_NAMESPACE);
}


} } // namespace Poco::XML
