//
// XMLWriter.cpp
//
// $Id: //poco/1.4/XML/src/XMLWriter.cpp#5 $
//
// Library: XML
// Package: XML
// Module:  XMLWriter
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/XML/XMLWriter.h"
#include "Poco/XML/XMLString.h"
#include "Poco/XML/XMLException.h"
#include "Poco/SAX/AttributesImpl.h"
#include "Poco/UTF8Encoding.h"
#include "Poco/UTF16Encoding.h"
#include <sstream>


namespace Poco {
namespace XML {


const std::string XMLWriter::NEWLINE_DEFAULT;
const std::string XMLWriter::NEWLINE_CR         = "\r";
const std::string XMLWriter::NEWLINE_CRLF       = "\r\n";
const std::string XMLWriter::NEWLINE_LF         = "\n";
const std::string XMLWriter::MARKUP_QUOTENC     = "&quot;";
const std::string XMLWriter::MARKUP_AMPENC      = "&amp;";
const std::string XMLWriter::MARKUP_LTENC       = "&lt;";
const std::string XMLWriter::MARKUP_GTENC       = "&gt;";
const std::string XMLWriter::MARKUP_TABENC      = "&#x9;";
const std::string XMLWriter::MARKUP_CRENC       = "&#xD;";
const std::string XMLWriter::MARKUP_LFENC       = "&#xA;";
const std::string XMLWriter::MARKUP_LT          = "<";
const std::string XMLWriter::MARKUP_GT          = ">";
const std::string XMLWriter::MARKUP_SLASHGT     = "/>";
const std::string XMLWriter::MARKUP_LTSLASH     = "</";
const std::string XMLWriter::MARKUP_COLON       = ":";
const std::string XMLWriter::MARKUP_EQQUOT      = "=\"";
const std::string XMLWriter::MARKUP_QUOT        = "\"";
const std::string XMLWriter::MARKUP_SPACE       = " ";
const std::string XMLWriter::MARKUP_TAB         = "\t";
const std::string XMLWriter::MARKUP_BEGIN_CDATA = "<![CDATA[";
const std::string XMLWriter::MARKUP_END_CDATA   = "]]>";


#if defined(XML_UNICODE_WCHAR_T)
	#define NATIVE_ENCODING Poco::UTF16Encoding
#else
	#define NATIVE_ENCODING Poco::UTF8Encoding
#endif


XMLWriter::XMLWriter(XMLByteOutputStream& str, int options):
	_pTextConverter(0),
	_pInEncoding(new NATIVE_ENCODING),
	_pOutEncoding(new Poco::UTF8Encoding),
	_options(options),
	_encoding("UTF-8"),
	_depth(-1),
	_elementCount(0),
	_inFragment(false),
	_inCDATA(false),
	_inDTD(false),
	_inInternalDTD(false),
	_contentWritten(false),
	_unclosedStartTag(false),
	_prefix(0),
	_nsContextPushed(false),
	_indent(MARKUP_TAB)
{
	_pTextConverter = new Poco::OutputStreamConverter(str, *_pInEncoding, *_pOutEncoding);
	setNewLine((_options & CANONICAL_XML) ? NEWLINE_LF : NEWLINE_DEFAULT);
}


XMLWriter::XMLWriter(XMLByteOutputStream& str, int options, const std::string& encodingName, Poco::TextEncoding& textEncoding):
	_pTextConverter(0),
	_pInEncoding(new NATIVE_ENCODING),
	_pOutEncoding(0),
	_options(options),
	_encoding(encodingName),
	_depth(-1),
	_elementCount(0),
	_inFragment(false),
	_inCDATA(false),
	_inDTD(false),
	_inInternalDTD(false),
	_contentWritten(false),
	_unclosedStartTag(false),
	_prefix(0),
	_nsContextPushed(false),
	_indent(MARKUP_TAB)
{
	_pTextConverter = new Poco::OutputStreamConverter(str, *_pInEncoding, textEncoding);
	setNewLine((_options & CANONICAL_XML) ? NEWLINE_LF : NEWLINE_DEFAULT);
}


XMLWriter::XMLWriter(XMLByteOutputStream& str, int options, const std::string& encodingName, Poco::TextEncoding* pTextEncoding):
	_pTextConverter(0),
	_pInEncoding(new NATIVE_ENCODING),
	_pOutEncoding(0),
	_options(options),
	_encoding(encodingName),
	_depth(-1),
	_elementCount(0),
	_inFragment(false),
	_inCDATA(false),
	_inDTD(false),
	_inInternalDTD(false),
	_contentWritten(false),
	_unclosedStartTag(false),
	_prefix(0),
	_nsContextPushed(false),
	_indent(MARKUP_TAB)
{
	if (pTextEncoding)
	{
		_pTextConverter = new Poco::OutputStreamConverter(str, *_pInEncoding, *pTextEncoding);
	}
	else
	{
		_encoding = "UTF-8";
		_pOutEncoding = new Poco::UTF8Encoding;
		_pTextConverter = new Poco::OutputStreamConverter(str, *_pInEncoding, *_pOutEncoding);
	}
	setNewLine((_options & CANONICAL_XML) ? NEWLINE_LF : NEWLINE_DEFAULT);
}


XMLWriter::~XMLWriter()
{
	delete _pTextConverter;
	delete _pInEncoding;
	delete _pOutEncoding;
}


void XMLWriter::setDocumentLocator(const Locator* loc)
{
}


void XMLWriter::setNewLine(const std::string& newLineCharacters)
{
	if (newLineCharacters.empty())
	{
#if defined(_WIN32)
		_newLine = NEWLINE_CRLF;
#else
		_newLine = NEWLINE_LF;
#endif
	}
	else _newLine = newLineCharacters;
}


const std::string& XMLWriter::getNewLine() const
{
	return _newLine;
}


void XMLWriter::setIndent(const std::string& indent)
{
	_indent = indent;
}


const std::string& XMLWriter::getIndent() const
{
	return _indent;
}


void XMLWriter::startDocument()
{
	if (_depth != -1)
		throw XMLException("Cannot start a document in another document");

	_inFragment    = false;
	_depth         = 0;
	_elementCount  = 0;
	_inDTD         = false;
	_inInternalDTD = false;
	_prefix        = 0;

	if (_options & WRITE_XML_DECLARATION)
		writeXMLDeclaration();
	
	_contentWritten = true;
	_namespaces.reset();
	_namespaces.pushContext();
}


void XMLWriter::endDocument()
{
	if (_depth > 0)
		throw XMLException("Not well-formed (at least one tag has no matching end tag)");
	if (_elementCount == 0)
		throw XMLException("No document element");

	poco_assert_dbg (!_unclosedStartTag);

	_elementCount = 0;
	_depth        = -1;
}


void XMLWriter::startFragment()
{
	if (_depth != -1)
		throw XMLException("Cannot start a fragment in another fragment or document");

	_inFragment   = true;
	_depth        = 0;
	_elementCount = 0;
	_prefix       = 0;

	_contentWritten = true;
	_namespaces.reset();
	_namespaces.pushContext();
}


void XMLWriter::endFragment()
{
	if (_depth > 1)
		throw XMLException("Not well-formed (at least one tag has no matching end tag)");
	
	_inFragment   = false;
	_elementCount = 0;
	_depth        = -1;
}


void XMLWriter::startElement(const XMLString& namespaceURI, const XMLString& localName, const XMLString& qname)
{
	const AttributesImpl attributes;
	startElement(namespaceURI, localName, qname, attributes);
}


void XMLWriter::startElement(const XMLString& namespaceURI, const XMLString& localName, const XMLString& qname, const Attributes& attributes)
{
	if (_depth == 0 && !_inFragment && _elementCount > 1) 
		throw XMLException("Not well-formed. Second root element found", nameToString(localName, qname));
	
	if (_unclosedStartTag) closeStartTag();
	prettyPrint();
	writeStartElement(namespaceURI, localName, qname, attributes);
	_elementStack.push_back(Name(qname, namespaceURI, localName));
	_contentWritten = false;
	++_depth;
}


void XMLWriter::endElement(const XMLString& namespaceURI, const XMLString& localName, const XMLString& qname)
{
	if (_depth < 1)
		throw XMLException("No unclosed tag");

	if (!_elementStack.back().equalsWeakly(qname, namespaceURI, localName))
		throw XMLException("End tag does not match start tag", nameToString(localName, qname));

	_elementStack.pop_back();
	--_depth;
	if (!_unclosedStartTag) prettyPrint();
	writeEndElement(namespaceURI, localName, qname);
	_contentWritten = false;
	if (_depth == 0)
		writeNewLine();
}


void XMLWriter::emptyElement(const XMLString& namespaceURI, const XMLString& localName, const XMLString& qname)
{
	const AttributesImpl attributes;
	emptyElement(namespaceURI, localName, qname, attributes);
}


void XMLWriter::emptyElement(const XMLString& namespaceURI, const XMLString& localName, const XMLString& qname, const Attributes& attributes)
{
	if (_depth == 0 && _elementCount > 1)
		throw XMLException("Not well-formed. Second root element found.");

	if (_unclosedStartTag) closeStartTag();
	prettyPrint();
	writeStartElement(namespaceURI, localName, qname, attributes);
	_contentWritten = false;
	writeMarkup("/");
	closeStartTag();
	_namespaces.popContext();
}


void XMLWriter::characters(const XMLChar ch[], int start, int length)
{
	if (length == 0) return;

	if (_unclosedStartTag) closeStartTag();
	_contentWritten = _contentWritten || length > 0;
	if (_inCDATA)
	{
		while (length-- > 0) writeXML(ch[start++]);
	}
	else
	{
		while (length-- > 0)
		{
			XMLChar c = ch[start++];
			switch (c)
			{
			case '"':  writeMarkup(MARKUP_QUOTENC); break;
			case '&':  writeMarkup(MARKUP_AMPENC); break;
			case '<':  writeMarkup(MARKUP_LTENC); break;
			case '>':  writeMarkup(MARKUP_GTENC); break;
			default:
				if (c >= 0 && c < 32)
				{
					if (c == '\t' || c == '\r' || c == '\n')
						writeXML(c);
					else
						throw XMLException("Invalid character token.");
				}
				else writeXML(c);
			}
		}
	}
}


void XMLWriter::characters(const XMLString& str)
{
	characters(str.data(), 0, (int) str.length());
}


void XMLWriter::rawCharacters(const XMLString& str)
{
	if (_unclosedStartTag) closeStartTag();
	_contentWritten = _contentWritten || !str.empty();
	writeXML(str);
}


void XMLWriter::ignorableWhitespace(const XMLChar ch[], int start, int length)
{
	characters(ch, start, length);
}


void XMLWriter::processingInstruction(const XMLString& target, const XMLString& data)
{
	if (_unclosedStartTag) closeStartTag();
	prettyPrint();
	writeMarkup("<?");
	writeXML(target);
	if (!data.empty())
	{
		writeMarkup(MARKUP_SPACE);
		writeXML(data);
	}
	writeMarkup("?>");
	if (_depth == 0)
		writeNewLine();
}


namespace
{
	static const XMLString CDATA = toXMLString("CDATA");
}


void XMLWriter::dataElement(const XMLString& namespaceURI, const XMLString& localName, const XMLString& qname,
                             const XMLString& data,
	                         const XMLString& attr1, const XMLString& value1,
							 const XMLString& attr2, const XMLString& value2,
							 const XMLString& attr3, const XMLString& value3)
{
	AttributesImpl attributes;
	if (!attr1.empty()) attributes.addAttribute(XMLString(), XMLString(), attr1, CDATA, value1);
	if (!attr2.empty()) attributes.addAttribute(XMLString(), XMLString(), attr2, CDATA, value2);
	if (!attr3.empty()) attributes.addAttribute(XMLString(), XMLString(), attr3, CDATA, value3);
	if (data.empty())
	{
		emptyElement(namespaceURI, localName, qname, attributes);
	}
	else
	{
		startElement(namespaceURI, localName, qname, attributes);
		characters(data);
		endElement(namespaceURI, localName, qname);
	}
}


void XMLWriter::startPrefixMapping(const XMLString& prefix, const XMLString& namespaceURI)
{
	if (prefix != NamespaceSupport::XML_NAMESPACE_PREFIX)
	{
		if (!_nsContextPushed)
		{
			_namespaces.pushContext();
			_nsContextPushed = true;
		}
		_namespaces.declarePrefix(prefix, namespaceURI);
	}
}


void XMLWriter::endPrefixMapping(const XMLString& prefix)
{
	// Note: prefix removed by popContext() at element closing tag
}


void XMLWriter::skippedEntity(const XMLString& name)
{
}


void XMLWriter::startCDATA()
{
	if (_inCDATA) throw XMLException("Cannot nest CDATA sections");
	if (_unclosedStartTag) closeStartTag();
	_inCDATA = true;
	writeMarkup(MARKUP_BEGIN_CDATA);
}


void XMLWriter::endCDATA()
{
	poco_assert (_inCDATA);
	_inCDATA = false;
	writeMarkup(MARKUP_END_CDATA);
}


void XMLWriter::comment(const XMLChar ch[], int start, int length)
{
	if (_unclosedStartTag) closeStartTag();
	prettyPrint();
	writeMarkup("<!--");
	while (length-- > 0) writeXML(ch[start++]);
	writeMarkup("-->");
	_contentWritten = false;
}


void XMLWriter::startDTD(const XMLString& name, const XMLString& publicId, const XMLString& systemId)
{
	writeMarkup("<!DOCTYPE ");
	writeXML(name);
	if (!publicId.empty())
	{
		writeMarkup(" PUBLIC \"");
		writeXML(publicId);
		writeMarkup("\"");
	}
	if (!systemId.empty())
	{
		if (publicId.empty())
		{
			writeMarkup(" SYSTEM");
		}
		writeMarkup(" \"");
		writeXML(systemId);
		writeMarkup("\"");
	}
	_inDTD = true;
}


void XMLWriter::endDTD()
{
	poco_assert (_inDTD);
	if (_inInternalDTD)
	{
		writeNewLine();
		writeMarkup("]");
		_inInternalDTD = false;
	}
	writeMarkup(">");
	writeNewLine();
	_inDTD = false;
}


void XMLWriter::startEntity(const XMLString& name)
{
}


void XMLWriter::endEntity(const XMLString& name)
{
}


void XMLWriter::notationDecl(const XMLString& name, const XMLString* publicId, const XMLString* systemId)
{
	if (!_inDTD) throw XMLException("Notation declaration not within DTD");
	if (!_inInternalDTD)
	{
		writeMarkup(" [");
		_inInternalDTD = true;
	}
	if (_options & PRETTY_PRINT)
	{
		writeNewLine();
		writeMarkup(_indent);
	}
	writeMarkup("<!NOTATION ");
	writeXML(name);
	if (systemId && !systemId->empty())
	{
		writeMarkup(" SYSTEM \"");
		writeXML(*systemId);
		writeMarkup("\"");
	}
	if (publicId && !publicId->empty())
	{
		writeMarkup(" PUBLIC \"");
		writeXML(*publicId);
		writeMarkup("\"");
	}
	writeMarkup(">");
}


void XMLWriter::unparsedEntityDecl(const XMLString& name, const XMLString* publicId, const XMLString& systemId, const XMLString& notationName)
{
	if (!_inDTD) throw XMLException("Entity declaration not within DTD");
	if (!_inInternalDTD)
	{
		writeMarkup(" [");
		_inInternalDTD = true;
	}
	if (_options & PRETTY_PRINT)
	{
		writeNewLine();
		writeMarkup(_indent);
	}
	writeMarkup("<!ENTITY ");
	writeXML(name);
	if (!systemId.empty())
	{
		writeMarkup(" SYSTEM \"");
		writeXML(systemId);
		writeMarkup("\"");
	}
	if (publicId && !publicId->empty())
	{
		writeMarkup(" PUBLIC \"");
		writeXML(*publicId);
		writeMarkup("\"");
	}
	if (!notationName.empty())
	{
		writeMarkup(" NDATA ");
		writeXML(notationName);
	}
	writeMarkup(">");
}


void XMLWriter::prettyPrint() const
{
	if ((_options & PRETTY_PRINT) && !_contentWritten)
	{
		writeNewLine();
		writeIndent();
	}
}


void XMLWriter::writeStartElement(const XMLString& namespaceURI, const XMLString& localName, const XMLString& qname, const Attributes& attributes)
{
	if (!_nsContextPushed)
		_namespaces.pushContext();
	_nsContextPushed = false;
	++_elementCount;
	
	declareAttributeNamespaces(attributes);

	writeMarkup(MARKUP_LT);
	if (!localName.empty() && (qname.empty() || localName == qname))
	{
		XMLString prefix;
		if (!namespaceURI.empty() && !_namespaces.isMapped(namespaceURI))
		{
			prefix = uniquePrefix();
			_namespaces.declarePrefix(prefix, namespaceURI);
		}
		else prefix = _namespaces.getPrefix(namespaceURI);
		writeName(prefix, localName);
	}
	else if (namespaceURI.empty() && localName.empty() && !qname.empty())
	{
		writeXML(qname);
	}
	else if (!localName.empty() && !qname.empty())
	{
		XMLString local;
		XMLString prefix;
		Name::split(qname, prefix, local);
		if (prefix.empty()) prefix = _namespaces.getPrefix(namespaceURI);
		const XMLString& uri = _namespaces.getURI(prefix);
		if ((uri.empty() || uri != namespaceURI) && !namespaceURI.empty())
		{
			_namespaces.declarePrefix(prefix, namespaceURI);
		}
		writeName(prefix, localName);
	}
	else throw XMLException("Tag mismatch", nameToString(localName, qname));

	AttributeMap attributeMap;
	addNamespaceAttributes(attributeMap);
	addAttributes(attributeMap, attributes, namespaceURI);
	writeAttributes(attributeMap);
	_unclosedStartTag = true;
}


void XMLWriter::writeEndElement(const XMLString& namespaceURI, const XMLString& localName, const XMLString& qname)
{
	if (_unclosedStartTag && !(_options & CANONICAL_XML))
	{
		writeMarkup(MARKUP_SLASHGT);
		_unclosedStartTag = false;
	}
	else
	{
		if (_unclosedStartTag)
		{
			writeMarkup(MARKUP_GT);
			_unclosedStartTag = false;
		}
		writeMarkup(MARKUP_LTSLASH);
		if (!localName.empty())
		{
			XMLString prefix = _namespaces.getPrefix(namespaceURI);
			writeName(prefix, localName);
		}
		else
		{
			writeXML(qname);
		}
		writeMarkup(MARKUP_GT);
	}
	_namespaces.popContext();
}


void XMLWriter::closeStartTag()
{
	_unclosedStartTag = false;
	writeMarkup(MARKUP_GT);
}


void XMLWriter::declareAttributeNamespaces(const Attributes& attributes)
{
	for (int i = 0; i < attributes.getLength(); i++)
	{
		XMLString namespaceURI = attributes.getURI(i);
		XMLString localName    = attributes.getLocalName(i);
		XMLString qname        = attributes.getQName(i);
		if (!localName.empty())
		{
			XMLString prefix;
			XMLString splitLocalName;
			Name::split(qname, prefix, splitLocalName);
			if (prefix.empty()) prefix = _namespaces.getPrefix(namespaceURI);
			if (prefix.empty() && !namespaceURI.empty() && !_namespaces.isMapped(namespaceURI))
			{
				prefix = uniquePrefix();
				_namespaces.declarePrefix(prefix, namespaceURI);
			}

			const XMLString& uri = _namespaces.getURI(prefix);
			if ((uri.empty() || uri != namespaceURI) && !namespaceURI.empty())
			{
				_namespaces.declarePrefix(prefix, namespaceURI);
			}
		}
	}
}


void XMLWriter::addNamespaceAttributes(AttributeMap& attributeMap)
{
	NamespaceSupport::PrefixSet prefixes;
	_namespaces.getDeclaredPrefixes(prefixes);
	for (NamespaceSupport::PrefixSet::const_iterator it = prefixes.begin(); it != prefixes.end(); ++it)
	{
		XMLString prefix = *it;
		XMLString uri    = _namespaces.getURI(prefix);
		XMLString qname  = NamespaceSupport::XMLNS_NAMESPACE_PREFIX;
		
		if (!prefix.empty())
		{
			qname.append(toXMLString(MARKUP_COLON));
			qname.append(prefix);
		}
		attributeMap[qname] = uri;
	}
}


void XMLWriter::addAttributes(AttributeMap& attributeMap, const Attributes& attributes, const XMLString& elementNamespaceURI)
{
	for (int i = 0; i < attributes.getLength(); i++)
	{
		XMLString namespaceURI = attributes.getURI(i);
		XMLString localName    = attributes.getLocalName(i);
		XMLString qname        = attributes.getQName(i);
		if (!localName.empty())
		{
			XMLString prefix;
			if (!namespaceURI.empty())
				prefix = _namespaces.getPrefix(namespaceURI);
			if (!prefix.empty())
			{
				qname = prefix;
				qname.append(toXMLString(MARKUP_COLON));
			}
			else qname.clear();
			qname.append(localName);
		}
		attributeMap[qname] = attributes.getValue(i);
	}
}


void XMLWriter::writeAttributes(const AttributeMap& attributeMap)
{
	for (AttributeMap::const_iterator it = attributeMap.begin(); it != attributeMap.end(); ++it)
	{
		if ((_options & PRETTY_PRINT) && (_options & PRETTY_PRINT_ATTRIBUTES))
		{
			writeNewLine();
			writeIndent(_depth + 1);
		}
		else
		{
			writeMarkup(MARKUP_SPACE);
		}
		writeXML(it->first);
		writeMarkup(MARKUP_EQQUOT);
		for (XMLString::const_iterator itc = it->second.begin(); itc != it->second.end(); ++itc)
		{
			XMLChar c = *itc;
			switch (c)
			{
			case '"':  writeMarkup(MARKUP_QUOTENC); break;
			case '&':  writeMarkup(MARKUP_AMPENC); break;
			case '<':  writeMarkup(MARKUP_LTENC); break;
			case '>':  writeMarkup(MARKUP_GTENC); break;
			case '\t': writeMarkup(MARKUP_TABENC); break;
			case '\r': writeMarkup(MARKUP_CRENC); break;
			case '\n': writeMarkup(MARKUP_LFENC); break;
			default:
				if (c >= 0 && c < 32)
					throw XMLException("Invalid character token.");
				else 
					writeXML(c);
			}
		}
		writeMarkup(MARKUP_QUOT);
	}
}


void XMLWriter::writeMarkup(const std::string& str) const
{
#if defined(XML_UNICODE_WCHAR_T)
	const XMLString xmlString = toXMLString(str);
	writeXML(xmlString);
#else
	_pTextConverter->write(str.data(), (int) str.size());
#endif
}


void XMLWriter::writeXML(const XMLString& str) const
{
	_pTextConverter->write((const char*) str.data(), (int) str.size()*sizeof(XMLChar));
}


void XMLWriter::writeXML(XMLChar ch) const
{
	_pTextConverter->write((const char*) &ch, sizeof(ch));
}


void XMLWriter::writeName(const XMLString& prefix, const XMLString& localName)
{
	if (prefix.empty())
	{
		writeXML(localName);
	}
	else
	{
		writeXML(prefix); 
		writeMarkup(MARKUP_COLON); 
		writeXML(localName);
	}
}


void XMLWriter::writeNewLine() const
{
	if (_options & PRETTY_PRINT)
		writeMarkup(_newLine);
}


void XMLWriter::writeIndent() const
{
	writeIndent(_depth);
}


void XMLWriter::writeIndent(int depth) const
{
	for (int i = 0; i < depth; ++i)
		writeMarkup(_indent);
}


void XMLWriter::writeXMLDeclaration()
{
	writeMarkup("<?xml version=\"1.0\"");
	if (!_encoding.empty())
	{
		writeMarkup(" encoding=\"");
		writeMarkup(_encoding);
		writeMarkup("\"");
	}
	writeMarkup("?>");
	writeNewLine();
}


std::string XMLWriter::nameToString(const XMLString& localName, const XMLString& qname)
{
	if (qname.empty())
		return fromXMLString(localName);
	else
		return fromXMLString(qname);
}


XMLString XMLWriter::uniquePrefix()
{
	std::ostringstream str;
	str << "ns" << ++_prefix;
	return toXMLString(str.str());
}


bool XMLWriter::isNamespaceMapped(const XMLString& namespc) const
{
	return _namespaces.isMapped(namespc);
}


} } // namespace Poco::XML
