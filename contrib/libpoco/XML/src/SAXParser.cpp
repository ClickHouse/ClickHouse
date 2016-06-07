//
// SAXParser.cpp
//
// $Id: //poco/1.4/XML/src/SAXParser.cpp#1 $
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


#include "Poco/SAX/SAXParser.h"
#include "Poco/SAX/SAXException.h"
#include "Poco/SAX/EntityResolverImpl.h"
#include "Poco/SAX/InputSource.h"
#include "Poco/XML/NamespaceStrategy.h"
#include <sstream>


namespace Poco {
namespace XML {


const XMLString SAXParser::FEATURE_PARTIAL_READS = toXMLString("http://www.appinf.com/features/enable-partial-reads");


SAXParser::SAXParser():
	_namespaces(true),
	_namespacePrefixes(false)
{
}


SAXParser::SAXParser(const XMLString& encoding):
	_engine(encoding),
	_namespaces(true),
	_namespacePrefixes(false)
{
}


SAXParser::~SAXParser()
{
}


void SAXParser::setEncoding(const XMLString& encoding)
{
	_engine.setEncoding(encoding);
}

	
const XMLString& SAXParser::getEncoding() const
{
	return _engine.getEncoding();
}


void SAXParser::addEncoding(const XMLString& name, Poco::TextEncoding* pEncoding)
{
	_engine.addEncoding(name, pEncoding);
}


void SAXParser::setEntityResolver(EntityResolver* pResolver)
{
	_engine.setEntityResolver(pResolver);
}


EntityResolver* SAXParser::getEntityResolver() const
{
	return _engine.getEntityResolver();
}


void SAXParser::setDTDHandler(DTDHandler* pDTDHandler)
{
	_engine.setDTDHandler(pDTDHandler);
}


DTDHandler* SAXParser::getDTDHandler() const
{
	return _engine.getDTDHandler();
}


void SAXParser::setContentHandler(ContentHandler* pContentHandler)
{
	_engine.setContentHandler(pContentHandler);
}


ContentHandler* SAXParser::getContentHandler() const
{
	return _engine.getContentHandler();
}


void SAXParser::setErrorHandler(ErrorHandler* pErrorHandler)
{
	_engine.setErrorHandler(pErrorHandler);
}


ErrorHandler* SAXParser::getErrorHandler() const
{
	return _engine.getErrorHandler();
}


void SAXParser::setFeature(const XMLString& featureId, bool state)
{
	if (featureId == XMLReader::FEATURE_VALIDATION || featureId == XMLReader::FEATURE_STRING_INTERNING)
		throw SAXNotSupportedException(fromXMLString(XMLReader::FEATURE_VALIDATION));
	else if (featureId == XMLReader::FEATURE_EXTERNAL_GENERAL_ENTITIES)
		_engine.setExternalGeneralEntities(state);
	else if (featureId == XMLReader::FEATURE_EXTERNAL_PARAMETER_ENTITIES)
		_engine.setExternalParameterEntities(state);
	else if (featureId == XMLReader::FEATURE_NAMESPACES)
		_namespaces = state;
	else if (featureId == XMLReader::FEATURE_NAMESPACE_PREFIXES)
		_namespacePrefixes = state;
	else if (featureId == FEATURE_PARTIAL_READS)
		_engine.setEnablePartialReads(state);
	else throw SAXNotRecognizedException(fromXMLString(featureId));
}


bool SAXParser::getFeature(const XMLString& featureId) const
{
	if (featureId == XMLReader::FEATURE_VALIDATION || featureId == XMLReader::FEATURE_STRING_INTERNING)
		throw SAXNotSupportedException(fromXMLString(XMLReader::FEATURE_VALIDATION));
	else if (featureId == XMLReader::FEATURE_EXTERNAL_GENERAL_ENTITIES)
		return _engine.getExternalGeneralEntities();
	else if (featureId == XMLReader::FEATURE_EXTERNAL_PARAMETER_ENTITIES)
		return _engine.getExternalParameterEntities();
	else if (featureId == XMLReader::FEATURE_NAMESPACES)
		return _namespaces;
	else if (featureId == XMLReader::FEATURE_NAMESPACE_PREFIXES)
		return _namespacePrefixes;
	else if (featureId == FEATURE_PARTIAL_READS)
		return _engine.getEnablePartialReads();
	else throw SAXNotRecognizedException(fromXMLString(featureId));
}


void SAXParser::setProperty(const XMLString& propertyId, const XMLString& value)
{
	if (propertyId == XMLReader::PROPERTY_DECLARATION_HANDLER || propertyId == XMLReader::PROPERTY_LEXICAL_HANDLER)
		throw SAXNotSupportedException(std::string("property does not take a string value: ") + fromXMLString(propertyId));
	else
		throw SAXNotRecognizedException(fromXMLString(propertyId));
}


void SAXParser::setProperty(const XMLString& propertyId, void* value)
{
	if (propertyId == XMLReader::PROPERTY_DECLARATION_HANDLER)
		_engine.setDeclHandler(reinterpret_cast<DeclHandler*>(value));
	else if (propertyId == XMLReader::PROPERTY_LEXICAL_HANDLER)
		_engine.setLexicalHandler(reinterpret_cast<LexicalHandler*>(value));
	else throw SAXNotRecognizedException(fromXMLString(propertyId));
}


void* SAXParser::getProperty(const XMLString& propertyId) const
{
	if (propertyId == XMLReader::PROPERTY_DECLARATION_HANDLER)
		return _engine.getDeclHandler();
	else if (propertyId == XMLReader::PROPERTY_LEXICAL_HANDLER)
		return _engine.getLexicalHandler();
	else throw SAXNotSupportedException(fromXMLString(propertyId));
}


void SAXParser::parse(InputSource* pInputSource)
{
	if (pInputSource->getByteStream() || pInputSource->getCharacterStream())
	{
		setupParse();
		_engine.parse(pInputSource);
	}
	else parse(pInputSource->getSystemId());
}


void SAXParser::parse(const XMLString& systemId)
{
	setupParse();
	EntityResolverImpl entityResolver;
	InputSource* pInputSource = entityResolver.resolveEntity(0, systemId);
	if (pInputSource)
	{
		try
		{
			_engine.parse(pInputSource);
		}
		catch (...)
		{
			entityResolver.releaseInputSource(pInputSource);
			throw;
		}
		entityResolver.releaseInputSource(pInputSource);
	}
	else throw XMLException("Cannot resolve system identifier", fromXMLString(systemId));
}


void SAXParser::parseString(const std::string& xml)
{
	parseMemoryNP(xml.data(), xml.size());
}


void SAXParser::parseMemoryNP(const char* xml, std::size_t size)
{
	setupParse();
	_engine.parse(xml, size);
}


void SAXParser::setupParse()
{
	if (_namespaces && !_namespacePrefixes)
		_engine.setNamespaceStrategy(new NoNamespacePrefixesStrategy);
	else if (_namespaces && _namespacePrefixes)
		_engine.setNamespaceStrategy(new NamespacePrefixesStrategy);
	else
		_engine.setNamespaceStrategy(new NoNamespacesStrategy);
}


} } // namespace Poco::XML
