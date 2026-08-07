//
// DOMParser.cpp
//
// Library: XML
// Package: DOM
// Module:  DOMParser
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/DOM/DOMParser.h"
#include "Poco/DOM/DOMBuilder.h"
#include "Poco/SAX/WhitespaceFilter.h"
#include "Poco/SAX/InputSource.h"
#include "Poco/XML/NamePool.h"
#include <sstream>


namespace Poco {
namespace XML {


const XMLString DOMParser::FEATURE_FILTER_WHITESPACE = toXMLString("http://www.appinf.com/features/no-whitespace-in-element-content");


DOMParser::DOMParser(NamePool* pNamePool):
	_pNamePool(pNamePool),
	_filterWhitespace(false)
{
	if (_pNamePool) _pNamePool->duplicate();
	_saxParser.setFeature(XMLReader::FEATURE_NAMESPACES, true);
	_saxParser.setFeature(XMLReader::FEATURE_NAMESPACE_PREFIXES, true);
}


DOMParser::DOMParser(unsigned long namePoolSize):
	_pNamePool(new NamePool(namePoolSize)),
	_filterWhitespace(false)
{
	_saxParser.setFeature(XMLReader::FEATURE_NAMESPACES, true);
	_saxParser.setFeature(XMLReader::FEATURE_NAMESPACE_PREFIXES, true);
}


DOMParser::~DOMParser()
{
	if (_pNamePool) _pNamePool->release();
}


void DOMParser::setEncoding(const XMLString& encoding)
{
	_saxParser.setEncoding(encoding);
}


const XMLString& DOMParser::getEncoding() const
{
	return _saxParser.getEncoding();
}


void DOMParser::addEncoding(const XMLString& name, Poco::TextEncoding* pEncoding)
{
	_saxParser.addEncoding(name, pEncoding);
}


void DOMParser::setFeature(const XMLString& name, bool state)
{
	if (name == FEATURE_FILTER_WHITESPACE)
		_filterWhitespace = state;
	else
		_saxParser.setFeature(name, state);
}


bool DOMParser::getFeature(const XMLString& name) const
{
	if (name == FEATURE_FILTER_WHITESPACE)
		return _filterWhitespace;
	else
		return _saxParser.getFeature(name);
}


Document* DOMParser::parse(const XMLString& uri)
{
	if (_filterWhitespace)
	{
		WhitespaceFilter filter(&_saxParser);
		DOMBuilder builder(filter, _pNamePool);
		return builder.parse(uri);
	}
	else
	{
		DOMBuilder builder(_saxParser, _pNamePool);
		return builder.parse(uri);
	}
}


Document* DOMParser::parse(InputSource* pInputSource)
{
	if (_filterWhitespace)
	{
		WhitespaceFilter filter(&_saxParser);
		DOMBuilder builder(filter, _pNamePool);
		return builder.parse(pInputSource);
	}
	else
	{
		DOMBuilder builder(_saxParser, _pNamePool);
		return builder.parse(pInputSource);
	}
}


Document* DOMParser::parseString(const std::string& xml)
{
	return parseMemory(xml.data(), xml.size());
}


Document* DOMParser::parseMemory(const char* xml, std::size_t size)
{
	if (_filterWhitespace)
	{
		WhitespaceFilter filter(&_saxParser);
		DOMBuilder builder(filter, _pNamePool);
		return builder.parseMemoryNP(xml, size);
	}
	else
	{
		DOMBuilder builder(_saxParser, _pNamePool);
		return builder.parseMemoryNP(xml, size);
	}
}


EntityResolver* DOMParser::getEntityResolver() const
{
	return _saxParser.getEntityResolver();
}


void DOMParser::setEntityResolver(EntityResolver* pEntityResolver)
{
	_saxParser.setEntityResolver(pEntityResolver);
}


} } // namespace Poco::XML
