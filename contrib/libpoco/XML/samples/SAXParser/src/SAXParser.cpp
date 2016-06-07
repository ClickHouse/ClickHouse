//
// SAXParser.cpp
//
// $Id: //poco/1.4/XML/samples/SAXParser/src/SAXParser.cpp#1 $
//
// This sample demonstrates the SAXParser class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/SAX/SAXParser.h"
#include "Poco/SAX/ContentHandler.h"
#include "Poco/SAX/LexicalHandler.h"
#include "Poco/SAX/Attributes.h"
#include "Poco/SAX/Locator.h"
#include "Poco/Exception.h"
#include <iostream>


using Poco::XML::SAXParser;
using Poco::XML::XMLReader;
using Poco::XML::XMLString;
using Poco::XML::XMLChar;
using Poco::XML::ContentHandler;
using Poco::XML::LexicalHandler;
using Poco::XML::Attributes;
using Poco::XML::Locator;


class MyHandler: public ContentHandler, public LexicalHandler
{
public:
	MyHandler():
		_pLocator(0)
	{
	}
	
	// ContentHandler
	void setDocumentLocator(const Locator* loc)
	{
		_pLocator = loc;
	}
	
	void startDocument()
	{
		where("startDocument");
	}
	
	void endDocument()
	{
		where("endDocument");
	}
	
	void startElement(const XMLString& uri, const XMLString& localName, const XMLString& qname, const Attributes& attributes)
	{
		where("startElement");
		std::cout << "uri:       " << uri << std::endl
		          << "localName: " << localName << std::endl
		          << "qname:     " << qname << std::endl;
		std::cout << "Attributes: " << std::endl;
		for (int i = 0; i < attributes.getLength(); ++i)
		{
			std::cout << attributes.getLocalName(i) << "=" << attributes.getValue(i) << std::endl;
		}
	}
	
	void endElement(const XMLString& uri, const XMLString& localName, const XMLString& qname)
	{
		where("endElement");
	}
	
	void characters(const XMLChar ch[], int start, int length)
	{
		where("characters");
		std::cout << std::string(ch + start, length) << std::endl;
	}
	
	void ignorableWhitespace(const XMLChar ch[], int start, int length)
	{
		where("ignorableWhitespace");
	}
	
	void processingInstruction(const XMLString& target, const XMLString& data)
	{
		where("processingInstruction");
		std::cout << "target=" << target << ", data=" << data << std::endl;
	}
	
	void startPrefixMapping(const XMLString& prefix, const XMLString& uri)
	{
		where("startPrefixMapping");
		std::cout << "prefix=" << prefix << " uri=" << uri << std::endl;
	}
	
	void endPrefixMapping(const XMLString& prefix)
	{
		where("endPrefixMapping");
		std::cout << "prefix=" << prefix << std::endl;
	}
	
	void skippedEntity(const XMLString& name)
	{
		where("skippedEntity");
		std::cout << "name=" << name << std::endl;
	}
	
	// LexicalHandler
	void startDTD(const XMLString& name, const XMLString& publicId, const XMLString& systemId)
	{
		where("startDTD");
	}
	
	void endDTD()
	{
		where("endDTD");
	}
	
	void startEntity(const XMLString& name)
	{
		where("startEntity");
	}
	
	void endEntity(const XMLString& name)
	{
		where("endEntity");
	}
	
	void startCDATA()
	{
		where("startCDATA");
	}
	
	void endCDATA()
	{
		where("endCDATA");
	}
	
	void comment(const XMLChar ch[], int start, int length)
	{
		where("comment");
	}
	
protected:
	void where(const std::string& meth)
	{
		std::cout << "*** " << meth;
		if (_pLocator)
		{
			std::cout << " in "
			          << _pLocator->getSystemId()
			          << ", line " << _pLocator->getLineNumber() 
			          << ", col " << _pLocator->getColumnNumber() 
			          << std::endl;
		}
	}
	
private:
	const Locator* _pLocator;
};


int main(int argc, char** argv)
{
	// parse an XML document and print the generated SAX events

	if (argc < 2)
	{
		std::cout << "usage: " << argv[0] << ": <xmlfile>" << std::endl;
		return 1;
	}
	
	MyHandler handler;

	SAXParser parser;
	parser.setFeature(XMLReader::FEATURE_NAMESPACES, true);
	parser.setFeature(XMLReader::FEATURE_NAMESPACE_PREFIXES, true);
	parser.setContentHandler(&handler);
	parser.setProperty(XMLReader::PROPERTY_LEXICAL_HANDLER, static_cast<LexicalHandler*>(&handler));
	
	try
	{
		parser.parse(argv[1]);
	}
	catch (Poco::Exception& e)
	{
		std::cerr << e.displayText() << std::endl;
		return 2;
	}
	
	return 0;
}
