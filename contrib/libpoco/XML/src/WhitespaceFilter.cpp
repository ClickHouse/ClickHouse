//
// WhitespaceFilter.cpp
//
// $Id: //poco/1.4/XML/src/WhitespaceFilter.cpp#1 $
//
// Library: XML
// Package: SAX
// Module:  WhitespaceFilter
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/SAX/WhitespaceFilter.h"
#include "Poco/SAX/SAXException.h"


namespace Poco {
namespace XML {


WhitespaceFilter::WhitespaceFilter():
	_pLexicalHandler(0),
	_filter(true)
{
}

	
WhitespaceFilter::WhitespaceFilter(XMLReader* pReader): 
	XMLFilterImpl(pReader),
	_pLexicalHandler(0),
	_filter(true)
{
}


WhitespaceFilter::~WhitespaceFilter()
{
}


void WhitespaceFilter::setProperty(const XMLString& propertyId, const XMLString& value)
{
	if (propertyId == XMLReader::PROPERTY_LEXICAL_HANDLER)
		throw SAXNotSupportedException(std::string("property does not take a string value: ") + fromXMLString(propertyId));
	else
		XMLFilterImpl::setProperty(propertyId, value);

}


void WhitespaceFilter::setProperty(const XMLString& propertyId, void* value)
{
	if (propertyId == XMLReader::PROPERTY_LEXICAL_HANDLER)
		_pLexicalHandler = reinterpret_cast<LexicalHandler*>(value);
	else
		XMLFilterImpl::setProperty(propertyId, value);
}


void* WhitespaceFilter::getProperty(const XMLString& propertyId) const
{
	if (propertyId == XMLReader::PROPERTY_LEXICAL_HANDLER)
		return _pLexicalHandler;
	else
		return XMLFilterImpl::getProperty(propertyId);
}


void WhitespaceFilter::startDocument()
{
	XMLFilterImpl::startDocument();
	_filter = true;
	_data.clear();
}


void WhitespaceFilter::endDocument()
{
	XMLFilterImpl::endDocument();
	_filter = true;
	_data.clear();
}


void WhitespaceFilter::startElement(const XMLString& uri, const XMLString& localName, const XMLString& qname, const Attributes& attrList)
{
	XMLFilterImpl::startElement(uri, localName, qname, attrList);
	_filter = true;
	_data.clear();
}


void WhitespaceFilter::endElement(const XMLString& uri, const XMLString& localName, const XMLString& qname)
{
	XMLFilterImpl::endElement(uri, localName, qname);
	_filter = true;
	_data.clear();
}


void WhitespaceFilter::characters(const XMLChar ch[], int start, int length)
{
	if (_filter)
	{
		bool ws = true;
		const XMLChar* it  = ch + start;
		const XMLChar* end = ch + start + length;
		_data.append(it, end);
		while (it != end)
		{
			if (*it != '\r' && *it != '\n' && *it != '\t' && *it != ' ')
			{
				ws = false;
				break;
			}
			++it;
		}
		if (!ws)
		{
			XMLFilterImpl::characters(_data.data(), 0, (int) _data.length());
			_filter = false;
			_data.clear();	
		}
	}
	else XMLFilterImpl::characters(ch, start, length);
}


void WhitespaceFilter::ignorableWhitespace(const XMLChar ch[], int start, int length)
{
	// the handler name already says that this data can be ignored
}


void WhitespaceFilter::processingInstruction(const XMLString& target, const XMLString& data)
{
	XMLFilterImpl::processingInstruction(target, data);
	_filter = true;
	_data.clear();
}


void WhitespaceFilter::startDTD(const XMLString& name, const XMLString& publicId, const XMLString& systemId)
{
	if (_pLexicalHandler)
		_pLexicalHandler->startDTD(name, publicId, systemId);
}


void WhitespaceFilter::endDTD()
{
	if (_pLexicalHandler)
		_pLexicalHandler->endDTD();
}


void WhitespaceFilter::startEntity(const XMLString& name)
{
	if (_pLexicalHandler)
		_pLexicalHandler->startEntity(name);
	_filter = true;
	_data.clear();
}


void WhitespaceFilter::endEntity(const XMLString& name)
{
	if (_pLexicalHandler)
		_pLexicalHandler->endEntity(name);
	_filter = true;
	_data.clear();
}


void WhitespaceFilter::startCDATA()
{
	if (_pLexicalHandler)
		_pLexicalHandler->startCDATA();
	_filter = false;
	_data.clear();
}


void WhitespaceFilter::endCDATA()
{
	if (_pLexicalHandler)
		_pLexicalHandler->endCDATA();
	_filter = true;
	_data.clear();
}


void WhitespaceFilter::comment(const XMLChar ch[], int start, int length)
{
	if (_pLexicalHandler)
		_pLexicalHandler->comment(ch, start, length);
	_filter = true;
	_data.clear();
}


void WhitespaceFilter::setupParse()
{
	XMLFilterImpl::setupParse();

	parent()->setProperty(XMLReader::PROPERTY_LEXICAL_HANDLER, static_cast<LexicalHandler*>(this));
}


} } // namespace Poco::XML
