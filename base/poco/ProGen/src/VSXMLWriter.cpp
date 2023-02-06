//
// VSXMLWriter.cpp
//
// Copyright (c) 2010, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "VSXMLWriter.h"
#include "Poco/SAX/AttributesImpl.h"
#include "Poco/String.h"


VSXMLWriter::VSXMLWriter(std::ostream& ostr, bool convertBool):
	_ostr(ostr),
	_convertBool(convertBool),
	_indent(0)
{
}


VSXMLWriter::~VSXMLWriter()
{
}

	
void VSXMLWriter::setDocumentLocator(const Poco::XML::Locator* loc)
{
}


void VSXMLWriter::startDocument()
{
	_ostr << "<?xml version=\"1.0\" encoding=\"Windows-1252\"?>\r\n";
	_tagClosed.push_back(true);
}


void VSXMLWriter::endDocument()
{
	_tagClosed.pop_back();
}


void VSXMLWriter::startFragment()
{
}


void VSXMLWriter::endFragment()
{
}


void VSXMLWriter::startElement(const Poco::XML::XMLString& namespaceURI, const Poco::XML::XMLString& localName, const Poco::XML::XMLString& qname, const Poco::XML::Attributes& attributes)
{
	if (!_tagClosed.back())
	{
		_ostr << ">\r\n";
		_tagClosed.back() = true;
	}
	indent();
	++_indent;
	_ostr << "<" << qname;
	if (attributes.getLength() > 0)
	{
		Poco::XML::AttributesImpl sortedAttributes;
		if (qname == "VisualStudioProject")
		{
			sortedAttributes.addAttribute("", "", "Name", "CDATA", attributes.getValue("Name"));
			sortedAttributes.addAttribute("", "", "Version", "CDATA", attributes.getValue("Version"));
			sortedAttributes.addAttribute("", "", "ProjectType", "CDATA", attributes.getValue("ProjectType"));
			sortedAttributes.addAttribute("", "", "ProjectGUID", "CDATA", attributes.getValue("ProjectGUID"));
			sortedAttributes.addAttribute("", "", "RootNamespace", "CDATA", attributes.getValue("RootNamespace"));
			sortedAttributes.addAttribute("", "", "Keyword", "CDATA", attributes.getValue("Keyword"));
		}
		else
		{
			if (attributes.getIndex("Name") != -1)
			{
				sortedAttributes.addAttribute("", "", "Name", "CDATA", attributes.getValue("Name"));
			}
			for (int i = 0; i < attributes.getLength(); i++)
			{
				if (attributes.getQName(i) != "Name")
				{
					std::string value = attributes.getValue(i);
					if (_convertBool && (value == "true" || value == "false"))	
						value = Poco::toUpper(value);
					sortedAttributes.addAttribute(attributes.getURI(i), attributes.getLocalName(i), attributes.getQName(i), attributes.getType(i), value);
				}
			}
		}
		for (int i = 0; i < sortedAttributes.getLength(); i++)
		{
			_ostr << "\r\n";
			indent();
			_ostr << sortedAttributes.getQName(i) << "=\"";
			std::string value = sortedAttributes.getValue(i);
			for (Poco::XML::XMLString::const_iterator itv = value.begin(); itv != value.end(); ++itv)
			{
				char c = *itv;
				switch (c)
				{
				case '"':  _ostr << "&quot;"; break;
				case '\'': _ostr << "&apos;"; break;
				case '&':  _ostr << "&amp;"; break;
				case '<':  _ostr << "&lt;"; break;
				case '>':  _ostr << "&gt;"; break;
				case '\t': _ostr << "&#x9;"; break;
				case '\r': _ostr << "&#xD;"; break;
				case '\n': _ostr << "&#xA;"; break;
				default:   _ostr << c; break;
				}
			}
			_ostr << "\"";
		}
	}
	_tagClosed.push_back(false);
}


void VSXMLWriter::endElement(const Poco::XML::XMLString& namespaceURI, const Poco::XML::XMLString& localName, const Poco::XML::XMLString& qname)
{
	--_indent;
	if (_tagClosed.back())
	{
		indent();
		_ostr << "</" << qname << ">\r\n";
	}
	else
	{
		_ostr << "/>\r\n";
	}
	_tagClosed.pop_back();
}


void VSXMLWriter::characters(const Poco::XML::XMLChar ch[], int start, int length)
{
	if (!_tagClosed.back())
	{
		_ostr << ">";
		_tagClosed.back() = true;
	}
	while (length-- > 0)
	{
		Poco::XML::XMLChar c = ch[start++];
		switch (c)
		{
		case '"':  _ostr << "&quot;"; break;
		case '\'': _ostr << "&apos;"; break;
		case '&':  _ostr << "&amp;"; break;
		case '<':  _ostr << "&lt;"; break;
		case '>':  _ostr << "&gt;"; break;
		default:   _ostr << c; break;
		}
	}
}


void VSXMLWriter::ignorableWhitespace(const Poco::XML::XMLChar ch[], int start, int length)
{
}


void VSXMLWriter::processingInstruction(const Poco::XML::XMLString& target, const Poco::XML::XMLString& data)
{
}


void VSXMLWriter::startPrefixMapping(const Poco::XML::XMLString& prefix, const Poco::XML::XMLString& namespaceURI)
{
}


void VSXMLWriter::endPrefixMapping(const Poco::XML::XMLString& prefix)
{
}


void VSXMLWriter::skippedEntity(const Poco::XML::XMLString& name)
{
}


void VSXMLWriter::indent()
{
	for (int i = 0; i < _indent; i++)
	{
		_ostr << "\t";
	}
}
