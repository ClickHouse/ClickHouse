//
// VSXMLWriter.h
//
// Definition of the VSXMLWriter class.
//
// Copyright (c) 2010, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef VSXMLWriter_INCLUDED
#define VSXMLWriter_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/SAX/ContentHandler.h"
#include "Poco/SAX/Locator.h"
#include "Poco/SAX/Attributes.h"
#include "Poco/XML/XMLString.h"
#include <vector>
#include <ostream>


class VSXMLWriter: public Poco::XML::ContentHandler
{
public:
	VSXMLWriter(std::ostream& ostr, bool convertBool);
	~VSXMLWriter();
		
	// ContentHandler
	void setDocumentLocator(const Poco::XML::Locator* loc);
	void startDocument();
	void endDocument();
	void startFragment();
	void endFragment();
	void startElement(const Poco::XML::XMLString& namespaceURI, const Poco::XML::XMLString& localName, const Poco::XML::XMLString& qname, const Poco::XML::Attributes& attributes);
	void endElement(const Poco::XML::XMLString& namespaceURI, const Poco::XML::XMLString& localName, const Poco::XML::XMLString& qname);
	void characters(const Poco::XML::XMLChar ch[], int start, int length);
	void ignorableWhitespace(const Poco::XML::XMLChar ch[], int start, int length);
	void processingInstruction(const Poco::XML::XMLString& target, const Poco::XML::XMLString& data);
	void startPrefixMapping(const Poco::XML::XMLString& prefix, const Poco::XML::XMLString& namespaceURI);
	void endPrefixMapping(const Poco::XML::XMLString& prefix);
	void skippedEntity(const Poco::XML::XMLString& name);

protected:
	void indent();

private:
	std::ostream& _ostr;
	bool _convertBool;
	int _indent;
	std::vector<bool> _tagClosed;
};


#endif // VSXMLWriter_INCLUDED
