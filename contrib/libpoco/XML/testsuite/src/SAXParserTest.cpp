//
// SAXParserTest.cpp
//
// $Id: //poco/1.4/XML/testsuite/src/SAXParserTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "SAXParserTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/SAX/SAXParser.h"
#include "Poco/SAX/InputSource.h"
#include "Poco/SAX/EntityResolver.h"
#include "Poco/SAX/SAXException.h"
#include "Poco/SAX/WhitespaceFilter.h"
#include "Poco/XML/XMLWriter.h"
#include "Poco/Latin9Encoding.h"
#include "Poco/FileStream.h"
#include <sstream>


using Poco::XML::SAXParser;
using Poco::XML::XMLWriter;
using Poco::XML::XMLReader;
using Poco::XML::InputSource;
using Poco::XML::EntityResolver;
using Poco::XML::XMLString;
using Poco::XML::SAXParseException;
using Poco::XML::WhitespaceFilter;


class TestEntityResolver: public EntityResolver
{
public:
	InputSource* resolveEntity(const XMLString* publicId, const XMLString& systemId)
	{
		if (systemId == "include.xml")
		{
			std::istringstream* istr = new std::istringstream(SAXParserTest::INCLUDE);
			InputSource* pIS = new InputSource(*istr);
			pIS->setSystemId(systemId);
			return pIS;
		}
		else if (systemId == "http://www.w3.org/TR/xhtml1/DTD/xhtml-lat1.ent")
		{
			std::istringstream* istr = new std::istringstream(SAXParserTest::XHTML_LATIN1_ENTITIES);
			InputSource* pIS = new InputSource(*istr);
			pIS->setSystemId(systemId);
			return pIS;
		}
		return 0;
	}
	
	void releaseInputSource(InputSource* pSource)
	{
		delete pSource->getByteStream();
		delete pSource;
	}
};


SAXParserTest::SAXParserTest(const std::string& name): CppUnit::TestCase(name)
{
}


SAXParserTest::~SAXParserTest()
{
}


void SAXParserTest::testSimple1()
{
	SAXParser parser;
	std::string xml = parse(parser, XMLWriter::CANONICAL, SIMPLE1);
	assert (xml == "<foo/>");
}


void SAXParserTest::testSimple2()
{
	SAXParser parser;
	std::string xml = parse(parser, XMLWriter::CANONICAL, SIMPLE2);
	assert (xml == "<foo/>");
}


void SAXParserTest::testAttributes()
{
	SAXParser parser;
	std::string xml = parse(parser, XMLWriter::CANONICAL, ATTRIBUTES);
	assert (xml == ATTRIBUTES);
}


void SAXParserTest::testCDATA()
{
	SAXParser parser;
	std::string xml = parse(parser, XMLWriter::CANONICAL, CDATA);
	assert (xml == CDATA);
}


void SAXParserTest::testComment()
{
	SAXParser parser;
	std::string xml = parse(parser, XMLWriter::CANONICAL, COMMENT);
	assert (xml == COMMENT);
}


void SAXParserTest::testPI()
{
	SAXParser parser;
	std::string xml = parse(parser, XMLWriter::CANONICAL, PROCESSING_INSTRUCTION);
	assert (xml == PROCESSING_INSTRUCTION);
}


void SAXParserTest::testDTD()
{
	SAXParser parser;
	std::string xml = parse(parser, XMLWriter::CANONICAL, DTD);
	assert (xml == "<!DOCTYPE test SYSTEM \"test.dtd\"><foo/>");
}


void SAXParserTest::testInternalEntity()
{
	SAXParser parser;
	std::string xml = parse(parser, XMLWriter::CANONICAL, INTERNAL_ENTITY);
	assert (xml ==	"<!DOCTYPE sample><root>\n\t<company>Applied Informatics</company>\n</root>");
}


void SAXParserTest::testNotation()
{
	SAXParser parser;
	std::string xml = parse(parser, XMLWriter::CANONICAL, NOTATION);
	assert (xml == "<!DOCTYPE test [<!NOTATION mov SYSTEM \"quicktime\">"
	               "<!NOTATION xml PUBLIC \"-//W3C//NOTATION XML 1.0//EN\">]>"
	               "<foo/>");
}


void SAXParserTest::testExternalUnparsed()
{
	SAXParser parser;
	std::string xml = parse(parser, XMLWriter::CANONICAL, EXTERNAL_UNPARSED);
	assert (xml == "<!DOCTYPE test [<!NOTATION mov SYSTEM \"quicktime\">"
	               "<!ENTITY movie SYSTEM \"movie.mov\" NDATA mov>]>"
	               "<sample/>");
}


void SAXParserTest::testExternalParsed()
{
	SAXParser parser;
	TestEntityResolver resolver;
	parser.setEntityResolver(&resolver);
	parser.setFeature(XMLReader::FEATURE_EXTERNAL_GENERAL_ENTITIES, true);
	std::string xml = parse(parser, XMLWriter::CANONICAL, EXTERNAL_PARSED);
	assert (xml == "<!DOCTYPE test><sample>\n\t<elem>\n\tAn external entity.\n</elem>\n\n</sample>");
}


void SAXParserTest::testDefaultNamespace()
{
	SAXParser parser;
	std::string xml = parse(parser, XMLWriter::CANONICAL, DEFAULT_NAMESPACE);
	assert (xml ==	DEFAULT_NAMESPACE);
}


void SAXParserTest::testNamespaces()
{
	SAXParser parser;
	parser.setFeature(XMLReader::FEATURE_NAMESPACES, true);
	parser.setFeature(XMLReader::FEATURE_NAMESPACE_PREFIXES, true);
	std::string xml = parse(parser, XMLWriter::CANONICAL, NAMESPACES);
	assert (xml == NAMESPACES);
}


void SAXParserTest::testNamespacesNoPrefixes()
{
	SAXParser parser;
	parser.setFeature(XMLReader::FEATURE_NAMESPACES, true);
	parser.setFeature(XMLReader::FEATURE_NAMESPACE_PREFIXES, false);
	std::string xml = parse(parser, XMLWriter::CANONICAL, NAMESPACES);
	assert (xml == NAMESPACES);
}


void SAXParserTest::testNoNamespaces()
{
	SAXParser parser;
	parser.setFeature(XMLReader::FEATURE_NAMESPACES, false);
	std::string xml = parse(parser, XMLWriter::CANONICAL, NAMESPACES);
	assert (xml == NAMESPACES);
}


void SAXParserTest::testUndeclaredNamespace()
{
	SAXParser parser;
	parser.setFeature(XMLReader::FEATURE_NAMESPACES, true);
	parser.setFeature(XMLReader::FEATURE_NAMESPACE_PREFIXES, true);
	try
	{
		std::string xml = parse(parser, XMLWriter::CANONICAL, UNDECLARED_NAMESPACE);
		fail("undeclared namespace - must throw exception");
	}
	catch (SAXParseException&)
	{
	}
}


void SAXParserTest::testUndeclaredNamespaceNoPrefixes()
{
	SAXParser parser;
	parser.setFeature(XMLReader::FEATURE_NAMESPACES, true);
	parser.setFeature(XMLReader::FEATURE_NAMESPACE_PREFIXES, false);
	try
	{
		std::string xml = parse(parser, XMLWriter::CANONICAL, UNDECLARED_NAMESPACE);
		fail("undeclared namespace - must throw exception");
	}
	catch (SAXParseException&)
	{
	}
}


void SAXParserTest::testUndeclaredNoNamespace()
{
	SAXParser parser;
	parser.setFeature(XMLReader::FEATURE_NAMESPACES, false);
	std::string xml = parse(parser, XMLWriter::CANONICAL, UNDECLARED_NAMESPACE);
	assert (xml == UNDECLARED_NAMESPACE);
}


void SAXParserTest::testRSS()
{
	SAXParser parser;
	WhitespaceFilter filter(&parser);
	TestEntityResolver resolver;
	filter.setEntityResolver(&resolver);
	parser.setFeature(XMLReader::FEATURE_EXTERNAL_GENERAL_ENTITIES, true);
	parser.setFeature(XMLReader::FEATURE_EXTERNAL_PARAMETER_ENTITIES, true);
	
	std::istringstream istr(RSS);
	Poco::FileOutputStream ostr("rss.xml");
	XMLWriter writer(ostr, XMLWriter::CANONICAL | XMLWriter::PRETTY_PRINT);
	filter.setContentHandler(&writer);
	filter.setDTDHandler(&writer);
	filter.setProperty(XMLReader::PROPERTY_LEXICAL_HANDLER, static_cast<Poco::XML::LexicalHandler*>(&writer));
	InputSource source(istr);
	filter.parse(&source);
}


void SAXParserTest::testEncoding()
{
	SAXParser parser;
	Poco::Latin9Encoding encoding;
	parser.addEncoding("ISO-8859-15", &encoding); 
	
	std::istringstream istr(ENCODING);
	std::ostringstream ostr;
	XMLWriter writer(ostr, XMLWriter::WRITE_XML_DECLARATION, "ISO-8859-15", encoding);
	parser.setContentHandler(&writer);
	parser.setDTDHandler(&writer);
	parser.setProperty(XMLReader::PROPERTY_LEXICAL_HANDLER, static_cast<Poco::XML::LexicalHandler*>(&writer));
	InputSource source(istr);
	parser.parse(&source);
	
	std::string xml = ostr.str();
	assert (xml == ENCODING);
}


void SAXParserTest::testCharacters()
{
	static const XMLString xml("<textnode> TEXT &amp; AMPERSAND </textnode>");
	SAXParser parser;
	parser.setFeature(XMLReader::FEATURE_NAMESPACES, false);
	std::string result = parse(parser, XMLWriter::CANONICAL, xml);
	assert (result == xml);
}


void SAXParserTest::testParseMemory()
{
	SAXParser parser;
	std::string xml = parseMemory(parser, XMLWriter::CANONICAL | XMLWriter::PRETTY_PRINT, WSDL);
	assert (xml == WSDL);
}


void SAXParserTest::testParsePartialReads()
{
	SAXParser parser;
	parser.setFeature("http://www.appinf.com/features/enable-partial-reads", true);

	std::string xml = parse(parser, XMLWriter::CANONICAL | XMLWriter::PRETTY_PRINT, WSDL);
	assert (xml == WSDL);
}


void SAXParserTest::setUp()
{
}


void SAXParserTest::tearDown()
{
}


std::string SAXParserTest::parse(XMLReader& reader, int options, const std::string& data)
{
	std::istringstream istr(data);
	std::ostringstream ostr;
	XMLWriter writer(ostr, options);
	writer.setNewLine(XMLWriter::NEWLINE_LF);
	reader.setContentHandler(&writer);
	reader.setDTDHandler(&writer);
	reader.setProperty(XMLReader::PROPERTY_LEXICAL_HANDLER, static_cast<Poco::XML::LexicalHandler*>(&writer));
	InputSource source(istr);
	reader.parse(&source);
	return ostr.str();
}


std::string SAXParserTest::parseMemory(XMLReader& reader, int options, const std::string& data)
{
	std::ostringstream ostr;
	XMLWriter writer(ostr, options);
	writer.setNewLine(XMLWriter::NEWLINE_LF);
	reader.setContentHandler(&writer);
	reader.setDTDHandler(&writer);
	reader.setProperty(XMLReader::PROPERTY_LEXICAL_HANDLER, static_cast<Poco::XML::LexicalHandler*>(&writer));
	reader.parseMemoryNP(data.data(), data.size());
	return ostr.str();
}


CppUnit::Test* SAXParserTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("SAXParserTest");

	CppUnit_addTest(pSuite, SAXParserTest, testSimple1);
	CppUnit_addTest(pSuite, SAXParserTest, testSimple2);
	CppUnit_addTest(pSuite, SAXParserTest, testAttributes);
	CppUnit_addTest(pSuite, SAXParserTest, testCDATA);
	CppUnit_addTest(pSuite, SAXParserTest, testComment);
	CppUnit_addTest(pSuite, SAXParserTest, testPI);
	CppUnit_addTest(pSuite, SAXParserTest, testDTD);
	CppUnit_addTest(pSuite, SAXParserTest, testInternalEntity);
	CppUnit_addTest(pSuite, SAXParserTest, testNotation);
	CppUnit_addTest(pSuite, SAXParserTest, testExternalUnparsed);
	CppUnit_addTest(pSuite, SAXParserTest, testExternalParsed);
	CppUnit_addTest(pSuite, SAXParserTest, testDefaultNamespace);
	CppUnit_addTest(pSuite, SAXParserTest, testNamespaces);
	CppUnit_addTest(pSuite, SAXParserTest, testNamespacesNoPrefixes);
	CppUnit_addTest(pSuite, SAXParserTest, testNoNamespaces);
	CppUnit_addTest(pSuite, SAXParserTest, testUndeclaredNamespace);
	CppUnit_addTest(pSuite, SAXParserTest, testUndeclaredNamespaceNoPrefixes);
	CppUnit_addTest(pSuite, SAXParserTest, testUndeclaredNoNamespace);
	CppUnit_addTest(pSuite, SAXParserTest, testRSS);
	CppUnit_addTest(pSuite, SAXParserTest, testEncoding);
	CppUnit_addTest(pSuite, SAXParserTest, testCharacters);
	CppUnit_addTest(pSuite, SAXParserTest, testParseMemory);
	CppUnit_addTest(pSuite, SAXParserTest, testParsePartialReads);

	return pSuite;
}


const std::string SAXParserTest::SIMPLE1 =
	"<foo/>\n";


const std::string SAXParserTest::SIMPLE2 =
	"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
	"<foo/>\n";


const std::string SAXParserTest::ATTRIBUTES =
	"<root a1=\"v1\">\n"
	"\t<elem a1=\"v1\" a2=\"v2\"/>\n"
	"</root>";


const std::string SAXParserTest::CDATA = 
	"<data>\n"
	"<![CDATA[\n"
	"\tThe following <tag attr=\"value\">is inside a CDATA section</tag>.\n"
	"]]>\n"
	"</data>";


const std::string SAXParserTest::COMMENT =
	"<!--this is a comment-->"
	"<root>\n"
	"\t<!--another comment-->\n"
	"\t<elem/>\n"
	"</root>";


const std::string SAXParserTest::PROCESSING_INSTRUCTION =
	"<html>\n"
	"\t<head>\n"
	"\t\t<?xml-stylesheet href=\"style.css\" type=\"text/css\"?>\n"
	"\t\t<title>test</title>\n"
	"\t</head>\n"
	"\t<body>\n"
	"\t\t<p>this is a test</p>\n"
	"\t</body>\n"
	"</html>";


const std::string SAXParserTest::DTD = 
	"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
	"<!DOCTYPE test SYSTEM \"test.dtd\">\n"
	"<foo/>";
	

const std::string SAXParserTest::INTERNAL_ENTITY =
    "<!DOCTYPE sample [\n"
    "\t<!ENTITY appinf \"Applied Informatics\">\n"
    "]>\n"
    "<root>\n"
    "\t<company>&appinf;</company>\n"
    "</root>";


const std::string SAXParserTest::NOTATION =
	"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
	"<!DOCTYPE test [\n"
	"\t<!NOTATION mov SYSTEM \"quicktime\">\n"
	"\t<!NOTATION xml PUBLIC \"-//W3C//NOTATION XML 1.0//EN\">\n"
	"]>\n"
	"<foo/>";


const std::string SAXParserTest::EXTERNAL_UNPARSED =
	"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
	"<!DOCTYPE test [\n"
	"\t<!NOTATION mov SYSTEM \"quicktime\">\n"
	"\t<!ENTITY movie SYSTEM \"movie.mov\" NDATA mov>\n"
	"]>\n"
	"<sample/>";


const std::string SAXParserTest::EXTERNAL_PARSED = 
	"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
	"<!DOCTYPE test [\n"
	"\t<!ENTITY include SYSTEM \"include.xml\">\n"
	"]>\n"
	"<sample>\n"
	"\t&include;\n"
	"</sample>\n";


const std::string SAXParserTest::INCLUDE = 
	"<elem>\n"
	"\tAn external entity.\n"
	"</elem>\n";


const std::string SAXParserTest::DEFAULT_NAMESPACE =
	"<root xmlns=\"urn:ns1\">\n"
	"\t<elem>data</elem>\n"
	"</root>";


const std::string SAXParserTest::NAMESPACES =
	"<ns1:root xmlns:ns1=\"urn:ns1\" xmlns:ns2=\"urn:ns2\">\n"
	"\t<ns2:elem>data</ns2:elem>\n"
	"\t<ns3:elem a1=\"v1\" ns2:a2=\"v2\" xmlns:ns3=\"urn:ns3\">\n"
	"\t\tmore data\n"
	"\t</ns3:elem>\n"
	"</ns1:root>";


const std::string SAXParserTest::UNDECLARED_NAMESPACE =
	"<ns1:root xmlns:ns1=\"urn:ns1\" xmlns:ns2=\"urn:ns2\">\n"
	"\t<ns2:elem>data</ns2:elem>\n"
	"\t<ns3:elem a1=\"v1\" ns2:a2=\"v2\" xmlns:ns3=\"urn:ns3\">\n"
	"\t\tmore data\n"
	"\t</ns3:elem>\n"
	"\t<ns4:elem/>\n"
	"</ns1:root>";


const std::string SAXParserTest::XHTML_LATIN1_ENTITIES =
	"<!-- Portions (C) International Organization for Standardization 1986\n"
	"     Permission to copy in any form is granted for use with\n"
	"     conforming SGML systems and applications as defined in\n"
	"     ISO 8879, provided this notice is included in all copies.\n"
	"-->\n"
	"<!-- Character entity set. Typical invocation:\n"
	"    <!ENTITY % HTMLlat1 PUBLIC\n"
	"       \"-//W3C//ENTITIES Latin 1 for XHTML//EN\"\n"
	"       \"http://www.w3.org/TR/xhtml1/DTD/xhtml-lat1.ent\">\n"
	"    %HTMLlat1;\n"
	"-->\n"
	"\n"
	"<!ENTITY nbsp   \"&#160;\"> <!-- no-break space = non-breaking space,\n"
	"                                  U+00A0 ISOnum -->\n"
	"<!ENTITY iexcl  \"&#161;\"> <!-- inverted exclamation mark, U+00A1 ISOnum -->\n"
	"<!ENTITY cent   \"&#162;\"> <!-- cent sign, U+00A2 ISOnum -->\n"
	"<!ENTITY pound  \"&#163;\"> <!-- pound sign, U+00A3 ISOnum -->\n"
	"<!ENTITY curren \"&#164;\"> <!-- currency sign, U+00A4 ISOnum -->\n"
	"<!ENTITY yen    \"&#165;\"> <!-- yen sign = yuan sign, U+00A5 ISOnum -->\n"
	"<!ENTITY brvbar \"&#166;\"> <!-- broken bar = broken vertical bar,\n"
	"                                  U+00A6 ISOnum -->\n"
	"<!ENTITY sect   \"&#167;\"> <!-- section sign, U+00A7 ISOnum -->\n"
	"<!ENTITY uml    \"&#168;\"> <!-- diaeresis = spacing diaeresis,\n"
	"                                  U+00A8 ISOdia -->\n"
	"<!ENTITY copy   \"&#169;\"> <!-- copyright sign, U+00A9 ISOnum -->\n"
	"<!ENTITY ordf   \"&#170;\"> <!-- feminine ordinal indicator, U+00AA ISOnum -->\n"
	"<!ENTITY laquo  \"&#171;\"> <!-- left-pointing double angle quotation mark\n"
	"                                  = left pointing guillemet, U+00AB ISOnum -->\n"
	"<!ENTITY not    \"&#172;\"> <!-- not sign = angled dash,\n"
	"                                  U+00AC ISOnum -->\n"
	"<!ENTITY shy    \"&#173;\"> <!-- soft hyphen = discretionary hyphen,\n"
	"                                  U+00AD ISOnum -->\n"
	"<!ENTITY reg    \"&#174;\"> <!-- registered sign = registered trade mark sign,\n"
	"                                  U+00AE ISOnum -->\n"
	"<!ENTITY macr   \"&#175;\"> <!-- macron = spacing macron = overline\n"
	"                                  = APL overbar, U+00AF ISOdia -->\n"
	"<!ENTITY deg    \"&#176;\"> <!-- degree sign, U+00B0 ISOnum -->\n"
	"<!ENTITY plusmn \"&#177;\"> <!-- plus-minus sign = plus-or-minus sign,\n"
	"                                  U+00B1 ISOnum -->\n"
	"<!ENTITY sup2   \"&#178;\"> <!-- superscript two = superscript digit two\n"
	"                                  = squared, U+00B2 ISOnum -->\n"
	"<!ENTITY sup3   \"&#179;\"> <!-- superscript three = superscript digit three\n"
	"                                  = cubed, U+00B3 ISOnum -->\n"
	"<!ENTITY acute  \"&#180;\"> <!-- acute accent = spacing acute,\n"
	"                                  U+00B4 ISOdia -->\n"
	"<!ENTITY micro  \"&#181;\"> <!-- micro sign, U+00B5 ISOnum -->\n"
	"<!ENTITY para   \"&#182;\"> <!-- pilcrow sign = paragraph sign,\n"
	"                                  U+00B6 ISOnum -->\n"
	"<!ENTITY middot \"&#183;\"> <!-- middle dot = Georgian comma\n"
	"                                  = Greek middle dot, U+00B7 ISOnum -->\n"
	"<!ENTITY cedil  \"&#184;\"> <!-- cedilla = spacing cedilla, U+00B8 ISOdia -->\n"
	"<!ENTITY sup1   \"&#185;\"> <!-- superscript one = superscript digit one,\n"
	"                                  U+00B9 ISOnum -->\n"
	"<!ENTITY ordm   \"&#186;\"> <!-- masculine ordinal indicator,\n"
	"                                  U+00BA ISOnum -->\n"
	"<!ENTITY raquo  \"&#187;\"> <!-- right-pointing double angle quotation mark\n"
	"                                  = right pointing guillemet, U+00BB ISOnum -->\n"
	"<!ENTITY frac14 \"&#188;\"> <!-- vulgar fraction one quarter\n"
	"                                  = fraction one quarter, U+00BC ISOnum -->\n"
	"<!ENTITY frac12 \"&#189;\"> <!-- vulgar fraction one half\n"
	"                                  = fraction one half, U+00BD ISOnum -->\n"
	"<!ENTITY frac34 \"&#190;\"> <!-- vulgar fraction three quarters\n"
	"                                  = fraction three quarters, U+00BE ISOnum -->\n"
	"<!ENTITY iquest \"&#191;\"> <!-- inverted question mark\n"
	"                                  = turned question mark, U+00BF ISOnum -->\n"
	"<!ENTITY Agrave \"&#192;\"> <!-- latin capital letter A with grave\n"
	"                                  = latin capital letter A grave,\n"
	"                                  U+00C0 ISOlat1 -->\n"
	"<!ENTITY Aacute \"&#193;\"> <!-- latin capital letter A with acute,\n"
	"                                  U+00C1 ISOlat1 -->\n"
	"<!ENTITY Acirc  \"&#194;\"> <!-- latin capital letter A with circumflex,\n"
	"                                  U+00C2 ISOlat1 -->\n"
	"<!ENTITY Atilde \"&#195;\"> <!-- latin capital letter A with tilde,\n"
	"                                  U+00C3 ISOlat1 -->\n"
	"<!ENTITY Auml   \"&#196;\"> <!-- latin capital letter A with diaeresis,\n"
	"                                  U+00C4 ISOlat1 -->\n"
	"<!ENTITY Aring  \"&#197;\"> <!-- latin capital letter A with ring above\n"
	"                                  = latin capital letter A ring,\n"
	"                                  U+00C5 ISOlat1 -->\n"
	"<!ENTITY AElig  \"&#198;\"> <!-- latin capital letter AE\n"
	"                                  = latin capital ligature AE,\n"
	"                                  U+00C6 ISOlat1 -->\n"
	"<!ENTITY Ccedil \"&#199;\"> <!-- latin capital letter C with cedilla,\n"
	"                                  U+00C7 ISOlat1 -->\n"
	"<!ENTITY Egrave \"&#200;\"> <!-- latin capital letter E with grave,\n"
	"                                  U+00C8 ISOlat1 -->\n"
	"<!ENTITY Eacute \"&#201;\"> <!-- latin capital letter E with acute,\n"
	"                                  U+00C9 ISOlat1 -->\n"
	"<!ENTITY Ecirc  \"&#202;\"> <!-- latin capital letter E with circumflex,\n"
	"                                  U+00CA ISOlat1 -->\n"
	"<!ENTITY Euml   \"&#203;\"> <!-- latin capital letter E with diaeresis,\n"
	"                                  U+00CB ISOlat1 -->\n"
	"<!ENTITY Igrave \"&#204;\"> <!-- latin capital letter I with grave,\n"
	"                                  U+00CC ISOlat1 -->\n"
	"<!ENTITY Iacute \"&#205;\"> <!-- latin capital letter I with acute,\n"
	"                                  U+00CD ISOlat1 -->\n"
	"<!ENTITY Icirc  \"&#206;\"> <!-- latin capital letter I with circumflex,\n"
	"                                  U+00CE ISOlat1 -->\n"
	"<!ENTITY Iuml   \"&#207;\"> <!-- latin capital letter I with diaeresis,\n"
	"                                  U+00CF ISOlat1 -->\n"
	"<!ENTITY ETH    \"&#208;\"> <!-- latin capital letter ETH, U+00D0 ISOlat1 -->\n"
	"<!ENTITY Ntilde \"&#209;\"> <!-- latin capital letter N with tilde,\n"
	"                                  U+00D1 ISOlat1 -->\n"
	"<!ENTITY Ograve \"&#210;\"> <!-- latin capital letter O with grave,\n"
	"                                  U+00D2 ISOlat1 -->\n"
	"<!ENTITY Oacute \"&#211;\"> <!-- latin capital letter O with acute,\n"
	"                                  U+00D3 ISOlat1 -->\n"
	"<!ENTITY Ocirc  \"&#212;\"> <!-- latin capital letter O with circumflex,\n"
	"                                  U+00D4 ISOlat1 -->\n"
	"<!ENTITY Otilde \"&#213;\"> <!-- latin capital letter O with tilde,\n"
	"                                  U+00D5 ISOlat1 -->\n"
	"<!ENTITY Ouml   \"&#214;\"> <!-- latin capital letter O with diaeresis,\n"
	"                                  U+00D6 ISOlat1 -->\n"
	"<!ENTITY times  \"&#215;\"> <!-- multiplication sign, U+00D7 ISOnum -->\n"
	"<!ENTITY Oslash \"&#216;\"> <!-- latin capital letter O with stroke\n"
	"                                  = latin capital letter O slash,\n"
	"                                  U+00D8 ISOlat1 -->\n"
	"<!ENTITY Ugrave \"&#217;\"> <!-- latin capital letter U with grave,\n"
	"                                  U+00D9 ISOlat1 -->\n"
	"<!ENTITY Uacute \"&#218;\"> <!-- latin capital letter U with acute,\n"
	"                                  U+00DA ISOlat1 -->\n"
	"<!ENTITY Ucirc  \"&#219;\"> <!-- latin capital letter U with circumflex,\n"
	"                                  U+00DB ISOlat1 -->\n"
	"<!ENTITY Uuml   \"&#220;\"> <!-- latin capital letter U with diaeresis,\n"
	"                                  U+00DC ISOlat1 -->\n"
	"<!ENTITY Yacute \"&#221;\"> <!-- latin capital letter Y with acute,\n"
	"                                  U+00DD ISOlat1 -->\n"
	"<!ENTITY THORN  \"&#222;\"> <!-- latin capital letter THORN,\n"
	"                                  U+00DE ISOlat1 -->\n"
	"<!ENTITY szlig  \"&#223;\"> <!-- latin small letter sharp s = ess-zed,\n"
	"                                  U+00DF ISOlat1 -->\n"
	"<!ENTITY agrave \"&#224;\"> <!-- latin small letter a with grave\n"
	"                                  = latin small letter a grave,\n"
	"                                  U+00E0 ISOlat1 -->\n"
	"<!ENTITY aacute \"&#225;\"> <!-- latin small letter a with acute,\n"
	"                                  U+00E1 ISOlat1 -->\n"
	"<!ENTITY acirc  \"&#226;\"> <!-- latin small letter a with circumflex,\n"
	"                                  U+00E2 ISOlat1 -->\n"
	"<!ENTITY atilde \"&#227;\"> <!-- latin small letter a with tilde,\n"
	"                                  U+00E3 ISOlat1 -->\n"
	"<!ENTITY auml   \"&#228;\"> <!-- latin small letter a with diaeresis,\n"
	"                                  U+00E4 ISOlat1 -->\n"
	"<!ENTITY aring  \"&#229;\"> <!-- latin small letter a with ring above\n"
	"                                  = latin small letter a ring,\n"
	"                                  U+00E5 ISOlat1 -->\n"
	"<!ENTITY aelig  \"&#230;\"> <!-- latin small letter ae\n"
	"                                  = latin small ligature ae, U+00E6 ISOlat1 -->\n"
	"<!ENTITY ccedil \"&#231;\"> <!-- latin small letter c with cedilla,\n"
	"                                  U+00E7 ISOlat1 -->\n"
	"<!ENTITY egrave \"&#232;\"> <!-- latin small letter e with grave,\n"
	"                                  U+00E8 ISOlat1 -->\n"
	"<!ENTITY eacute \"&#233;\"> <!-- latin small letter e with acute,\n"
	"                                  U+00E9 ISOlat1 -->\n"
	"<!ENTITY ecirc  \"&#234;\"> <!-- latin small letter e with circumflex,\n"
	"                                  U+00EA ISOlat1 -->\n"
	"<!ENTITY euml   \"&#235;\"> <!-- latin small letter e with diaeresis,\n"
	"                                  U+00EB ISOlat1 -->\n"
	"<!ENTITY igrave \"&#236;\"> <!-- latin small letter i with grave,\n"
	"                                  U+00EC ISOlat1 -->\n"
	"<!ENTITY iacute \"&#237;\"> <!-- latin small letter i with acute,\n"
	"                                  U+00ED ISOlat1 -->\n"
	"<!ENTITY icirc  \"&#238;\"> <!-- latin small letter i with circumflex,\n"
	"                                  U+00EE ISOlat1 -->\n"
	"<!ENTITY iuml   \"&#239;\"> <!-- latin small letter i with diaeresis,\n"
	"                                  U+00EF ISOlat1 -->\n"
	"<!ENTITY eth    \"&#240;\"> <!-- latin small letter eth, U+00F0 ISOlat1 -->\n"
	"<!ENTITY ntilde \"&#241;\"> <!-- latin small letter n with tilde,\n"
	"                                  U+00F1 ISOlat1 -->\n"
	"<!ENTITY ograve \"&#242;\"> <!-- latin small letter o with grave,\n"
	"                                  U+00F2 ISOlat1 -->\n"
	"<!ENTITY oacute \"&#243;\"> <!-- latin small letter o with acute,\n"
	"                                  U+00F3 ISOlat1 -->\n"
	"<!ENTITY ocirc  \"&#244;\"> <!-- latin small letter o with circumflex,\n"
	"                                  U+00F4 ISOlat1 -->\n"
	"<!ENTITY otilde \"&#245;\"> <!-- latin small letter o with tilde,\n"
	"                                  U+00F5 ISOlat1 -->\n"
	"<!ENTITY ouml   \"&#246;\"> <!-- latin small letter o with diaeresis,\n"
	"                                  U+00F6 ISOlat1 -->\n"
	"<!ENTITY divide \"&#247;\"> <!-- division sign, U+00F7 ISOnum -->\n"
	"<!ENTITY oslash \"&#248;\"> <!-- latin small letter o with stroke,\n"
	"                                  = latin small letter o slash,\n"
	"                                  U+00F8 ISOlat1 -->\n"
	"<!ENTITY ugrave \"&#249;\"> <!-- latin small letter u with grave,\n"
	"                                  U+00F9 ISOlat1 -->\n"
	"<!ENTITY uacute \"&#250;\"> <!-- latin small letter u with acute,\n"
	"                                  U+00FA ISOlat1 -->\n"
	"<!ENTITY ucirc  \"&#251;\"> <!-- latin small letter u with circumflex,\n"
	"                                  U+00FB ISOlat1 -->\n"
	"<!ENTITY uuml   \"&#252;\"> <!-- latin small letter u with diaeresis,\n"
	"                                  U+00FC ISOlat1 -->\n"
	"<!ENTITY yacute \"&#253;\"> <!-- latin small letter y with acute,\n"
	"                                  U+00FD ISOlat1 -->\n"
	"<!ENTITY thorn  \"&#254;\"> <!-- latin small letter thorn,\n"
	"                                  U+00FE ISOlat1 -->\n"
	"<!ENTITY yuml   \"&#255;\"> <!-- latin small letter y with diaeresis,\n"
	"                                  U+00FF ISOlat1 -->\n";

const std::string SAXParserTest::RSS =
	"<?xml version=\"1.0\"?>\n"
	"\n"
	"<!DOCTYPE rdf:RDF [\n"
	"<!ENTITY % HTMLlat1 PUBLIC\n"
	" \"-//W3C//ENTITIES Latin 1 for XHTML//EN\"\n"
	" \"http://www.w3.org/TR/xhtml1/DTD/xhtml-lat1.ent\">\n"
	"%HTMLlat1;\n"
	"]>\n"
	"\n"
	"<rdf:RDF \n"
	"  xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\" \n"
	"  xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
	"  xmlns:sy=\"http://purl.org/rss/1.0/modules/syndication/\"\n"
	"  xmlns=\"http://purl.org/rss/1.0/\"\n"
	"> \n"
	"\n"
	"  <channel rdf:about=\"http://meerkat.oreillynet.com/\">\n"
	"    <title>XML.com</title>   \n"
	"    <link>http://www.xml.com/</link>\n"
	"    <description>XML.com features a rich mix of information and services for the XML community.</description>\n"
	"    <sy:updatePeriod>hourly</sy:updatePeriod>\n"
	"    <sy:updateFrequency>2</sy:updateFrequency>\n"
	"    <sy:updateBase>2000-01-01T12:00+00:00</sy:updateBase>\n"
	"\n"
	"    <image rdf:resource=\"http://meerkat.oreillynet.com/icons/meerkat-powered.jpg\" />\n"
	"\n"
	"    <items>\n"
	"      <rdf:Seq>\n"
	"        <rdf:li rdf:resource=\"http://www.xml.com/pub/a/2005/02/09/xforms.html\" />\n"
	"        <rdf:li rdf:resource=\"http://www.xml.com/pub/a/2005/02/09/cssorxsl.html\" />\n"
	"        <rdf:li rdf:resource=\"http://www.xml.com/pub/a/2005/02/09/xml-http-request.html\" />\n"
	"        <rdf:li rdf:resource=\"http://www.xml.com/pub/a/2005/02/02/xpath2.html\" />\n"
	"        <rdf:li rdf:resource=\"http://www.xml.com/pub/a/2005/02/02/silent.html\" />\n"
	"        <rdf:li rdf:resource=\"http://www.xml.com/pub/a/2005/02/02/xpath2.html\" />\n"
	"        <rdf:li rdf:resource=\"http://www.xml.com/pub/a/2005/02/02/tmapi.html\" />\n"
	"        <rdf:li rdf:resource=\"http://www.xml.com/pub/a/2005/01/26/formtax.html\" />\n"
	"        <rdf:li rdf:resource=\"http://www.xml.com/pub/a/2005/01/26/hacking-ooo.html\" />\n"
	"        <rdf:li rdf:resource=\"http://www.xml.com/pub/a/2005/01/26/simile.html\" />\n"
	"        <rdf:li rdf:resource=\"http://www.xml.com/pub/a/2005/01/19/amara.html\" />\n"
	"        <rdf:li rdf:resource=\"http://www.xml.com/pub/a/2005/01/19/print.html\" />\n"
	"        <rdf:li rdf:resource=\"http://www.xml.com/pub/a/2005/01/19/review.html\" />\n"
	"        <rdf:li rdf:resource=\"http://www.xml.com/pub/a/2005/01/12/saml2.html\" />\n"
	"        <rdf:li rdf:resource=\"http://www.xml.com/pub/a/2005/01/12/comega.html\" />\n"
	"      </rdf:Seq>\n"
	"    </items>\n"
	"  \n"
	"    <textinput rdf:resource=\"http://meerkat.oreillynet.com/\" />\n"
	"\n"
	"  </channel>\n"
	"\n"
	"  <image rdf:about=\"http://meerkat.oreillynet.com/icons/meerkat-powered.jpg\">\n"
	"    <title>Meerkat Powered!</title>\n"
	"    <url>http://meerkat.oreillynet.com/icons/meerkat-powered.jpg</url>\n"
	"    <link>http://meerkat.oreillynet.com</link>\n"
	"  </image>\n"
	"\n"
	"  <item rdf:about=\"http://www.xml.com/pub/a/2005/02/09/xforms.html\">\n"
	"    <title>Features: Top 10 XForms Engines</title>\n"
	"    <link>http://www.xml.com/pub/a/2005/02/09/xforms.html</link>\n"
	"    <description>\n"
	"    Micah Dubinko, one of the gurus of XForms, offers a rundown on the state of XForms engines for 2005.\n"
	"   </description>\n"
	"    <dc:source>XML.com</dc:source>\n"
	"    <dc:creator>Micah Dubinko</dc:creator>\n"
	"    <dc:subject>Web, Applications</dc:subject>\n"
	"    <dc:publisher>O'Reilly Media, Inc.</dc:publisher>\n"
	"    <dc:date>2005-02-09</dc:date>\n"
	"    <dc:type>Features</dc:type>\n"
	"    <dc:format>text/html</dc:format>\n"
	"    <dc:language>en-us</dc:language>\n"
	"    <dc:rights>Copyright 2005, O'Reilly Media, Inc.</dc:rights>\n"
	"  </item>\n"
	"\n"
	"  <item rdf:about=\"http://www.xml.com/pub/a/2005/02/09/cssorxsl.html\">\n"
	"    <title>Features: Comparing CSS and XSL: A Reply from Norm Walsh</title>\n"
	"    <link>http://www.xml.com/pub/a/2005/02/09/cssorxsl.html</link>\n"
	"    <description>\n"
	"    Norm Walsh responds to a recent article about CSS and XSL, explaining how and when and why you'd want to use XSLFO or CSS or XSLT.\n"
	"   </description>\n"
	"    <dc:source>XML.com</dc:source>\n"
	"    <dc:creator>Norman Walsh</dc:creator>\n"
	"    <dc:subject>Style</dc:subject>\n"
	"    <dc:publisher>O'Reilly Media, Inc.</dc:publisher>\n"
	"    <dc:date>2005-02-09</dc:date>\n"
	"    <dc:type>Features</dc:type>\n"
	"    <dc:format>text/html</dc:format>\n"
	"    <dc:language>en-us</dc:language>\n"
	"    <dc:rights>Copyright 2005, O'Reilly Media, Inc.</dc:rights>\n"
	"  </item>\n"
	"\n"
	"  <item rdf:about=\"http://www.xml.com/pub/a/2005/02/09/xml-http-request.html\">\n"
	"    <title>Features: Very Dynamic Web Interfaces</title>\n"
	"    <link>http://www.xml.com/pub/a/2005/02/09/xml-http-request.html</link>\n"
	"    <description>\n"
	"    Drew McLellan explains how to use XMLHTTPRequest and Javascript to create web applications with very dynamic, smooth interfaces.\n"
	"   </description>\n"
	"    <dc:source>XML.com</dc:source>\n"
	"    <dc:creator>Drew McLellan</dc:creator>\n"
	"    <dc:subject>Web Development, Instruction</dc:subject>\n"
	"    <dc:publisher>O'Reilly Media, Inc.</dc:publisher>\n"
	"    <dc:date>2005-02-09</dc:date>\n"
	"    <dc:type>Features</dc:type>\n"
	"    <dc:format>text/html</dc:format>\n"
	"    <dc:language>en-us</dc:language>\n"
	"    <dc:rights>Copyright 2005, O'Reilly Media, Inc.</dc:rights>\n"
	"  </item>\n"
	"\n"
	"  <item rdf:about=\"http://www.xml.com/pub/a/2005/02/02/xpath2.html\">\n"
	"    <title>Transforming XML: The XPath 2.0 Data Model</title>\n"
	"    <link>http://www.xml.com/pub/a/2005/02/02/xpath2.html</link>\n"
	"    <description>\n"
	"    Bob DuCharme, in his latest Transforming XML column, examines the XPath 2.0, hence the XSLT 2.0, data model.\n"
	"   </description>\n"
	"    <dc:source>XML.com</dc:source>\n"
	"    <dc:creator>Bob DuCharme</dc:creator>\n"
	"    <dc:subject>Style, Style</dc:subject>\n"
	"    <dc:publisher>O'Reilly Media, Inc.</dc:publisher>\n"
	"    <dc:date>2005-02-02</dc:date>\n"
	"    <dc:type>Transforming XML</dc:type>\n"
	"    <dc:format>text/html</dc:format>\n"
	"    <dc:language>en-us</dc:language>\n"
	"    <dc:rights>Copyright 2005, O'Reilly Media, Inc.</dc:rights>\n"
	"  </item>\n"
	"\n"
	"  <item rdf:about=\"http://www.xml.com/pub/a/2005/02/02/silent.html\">\n"
	"    <title>XML Tourist: The Silent Soundtrack</title>\n"
	"    <link>http://www.xml.com/pub/a/2005/02/02/silent.html</link>\n"
	"    <description>\n"
	"    In this installation of XML Tourist, John E. Simpson presents an overview of the types of sound-to-text captioning available. Pinpointing closed captioning as the most suitable for use with computerized multimedia, he then explains how XML-based solutions address synchronization issues.\n"
	"   </description>\n"
	"    <dc:source>XML.com</dc:source>\n"
	"    <dc:creator>John E. Simpson</dc:creator>\n"
	"    <dc:subject>Graphics, Vertical Industries</dc:subject>\n"
	"    <dc:publisher>O'Reilly Media, Inc.</dc:publisher>\n"
	"    <dc:date>2005-02-02</dc:date>\n"
	"    <dc:type>XML Tourist</dc:type>\n"
	"    <dc:format>text/html</dc:format>\n"
	"    <dc:language>en-us</dc:language>\n"
	"    <dc:rights>Copyright 2005, O'Reilly Media, Inc.</dc:rights>\n"
	"  </item>\n"
	"\n"
	"  <item rdf:about=\"http://www.xml.com/pub/a/2005/02/02/xpath2.html\">\n"
	"    <title>Transforming XML: The XML 2.0 Data Model</title>\n"
	"    <link>http://www.xml.com/pub/a/2005/02/02/xpath2.html</link>\n"
	"    <description>\n"
	"    Bob DuCharme, in his latest Transforming XML column, examines the XPath 2.0, hence the XSLT 2.0, data model.\n"
	"   </description>\n"
	"    <dc:source>XML.com</dc:source>\n"
	"    <dc:creator>Bob DuCharme</dc:creator>\n"
	"    <dc:subject>Style, Style</dc:subject>\n"
	"    <dc:publisher>O'Reilly Media, Inc.</dc:publisher>\n"
	"    <dc:date>2005-02-02</dc:date>\n"
	"    <dc:type>Transforming XML</dc:type>\n"
	"    <dc:format>text/html</dc:format>\n"
	"    <dc:language>en-us</dc:language>\n"
	"    <dc:rights>Copyright 2005, O'Reilly Media, Inc.</dc:rights>\n"
	"  </item>\n"
	"\n"
	"  <item rdf:about=\"http://www.xml.com/pub/a/2005/02/02/tmapi.html\">\n"
	"    <title>Features: An Introduction to TMAPI</title>\n"
	"    <link>http://www.xml.com/pub/a/2005/02/02/tmapi.html</link>\n"
	"    <description>\n"
	"    TMAPI, a Java Topic Map API, is the standard way to interact with XML Topic Maps programmatically from Java. This article provides a tutorial for TMAPI. \n"
	"   </description>\n"
	"    <dc:source>XML.com</dc:source>\n"
	"    <dc:creator>Robert Barta, Oliver Leimig</dc:creator>\n"
	"    <dc:subject>Metadata, Metadata</dc:subject>\n"
	"    <dc:publisher>O'Reilly Media, Inc.</dc:publisher>\n"
	"    <dc:date>2005-02-02</dc:date>\n"
	"    <dc:type>Features</dc:type>\n"
	"    <dc:format>text/html</dc:format>\n"
	"    <dc:language>en-us</dc:language>\n"
	"    <dc:rights>Copyright 2005, O'Reilly Media, Inc.</dc:rights>\n"
	"  </item>\n"
	"\n"
	"  <item rdf:about=\"http://www.xml.com/pub/a/2005/01/26/formtax.html\">\n"
	"    <title>Features: Formal Taxonomies for the U.S. Government</title>\n"
	"    <link>http://www.xml.com/pub/a/2005/01/26/formtax.html</link>\n"
	"    <description>\n"
	"    Mike Daconta, Metadata Program Manager at the Department of Homeland Security, introduces the notion of a formal taxonomy in the context of the Federal Enteriprise Architecture's Data Reference Model.\n"
	"   </description>\n"
	"    <dc:source>XML.com</dc:source>\n"
	"    <dc:creator>Michael Daconta</dc:creator>\n"
	"    <dc:subject>Metadata, Metadata</dc:subject>\n"
	"    <dc:publisher>O'Reilly Media, Inc.</dc:publisher>\n"
	"    <dc:date>2005-01-26</dc:date>\n"
	"    <dc:type>Features</dc:type>\n"
	"    <dc:format>text/html</dc:format>\n"
	"    <dc:language>en-us</dc:language>\n"
	"    <dc:rights>Copyright 2005, O'Reilly Media, Inc.</dc:rights>\n"
	"  </item>\n"
	"\n"
	"  <item rdf:about=\"http://www.xml.com/pub/a/2005/01/26/hacking-ooo.html\">\n"
	"    <title>Features: Hacking Open Office</title>\n"
	"    <link>http://www.xml.com/pub/a/2005/01/26/hacking-ooo.html</link>\n"
	"    <description>\n"
	"    Peter Sefton shows us how to use XML tools to hack Open Office file formats.\n"
	"   </description>\n"
	"    <dc:source>XML.com</dc:source>\n"
	"    <dc:creator>Peter Sefton</dc:creator>\n"
	"    <dc:subject>Programming, Tools, Publishing</dc:subject>\n"
	"    <dc:publisher>O'Reilly Media, Inc.</dc:publisher>\n"
	"    <dc:date>2005-01-26</dc:date>\n"
	"    <dc:type>Features</dc:type>\n"
	"    <dc:format>text/html</dc:format>\n"
	"    <dc:language>en-us</dc:language>\n"
	"    <dc:rights>Copyright 2005, O'Reilly Media, Inc.</dc:rights>\n"
	"  </item>\n"
	"\n"
	"  <item rdf:about=\"http://www.xml.com/pub/a/2005/01/26/simile.html\">\n"
	"    <title>Features: SIMILE: Practical Metadata for the Semantic Web</title>\n"
	"    <link>http://www.xml.com/pub/a/2005/01/26/simile.html</link>\n"
	"    <description>\n"
	"    Digital libraries and generic metadata form part of the background assumptions and forward-looking goals of the Semantic Web. SIMILE is an interesting project aimed at realizing some of those goals.\n"
	"   </description>\n"
	"    <dc:source>XML.com</dc:source>\n"
	"    <dc:creator>Stephen Garland, Ryan Lee, Stefano Mazzocchi</dc:creator>\n"
	"    <dc:subject>Semantic Web, Metadata</dc:subject>\n"
	"    <dc:publisher>O'Reilly Media, Inc.</dc:publisher>\n"
	"    <dc:date>2005-01-26</dc:date>\n"
	"    <dc:type>Features</dc:type>\n"
	"    <dc:format>text/html</dc:format>\n"
	"    <dc:language>en-us</dc:language>\n"
	"    <dc:rights>Copyright 2005, O'Reilly Media, Inc.</dc:rights>\n"
	"  </item>\n"
	"\n"
	"  <item rdf:about=\"http://www.xml.com/pub/a/2005/01/19/amara.html\">\n"
	"    <title>Python and XML: Introducing the Amara XML Toolkit</title>\n"
	"    <link>http://www.xml.com/pub/a/2005/01/19/amara.html</link>\n"
	"    <description>\n"
	"    Uche Ogbuji introduces Amara, his new collection of XML tools for Python.\n"
	"   </description>\n"
	"    <dc:source>XML.com</dc:source>\n"
	"    <dc:creator>Uche Ogbuji</dc:creator>\n"
	"    <dc:subject>Programming, Programming</dc:subject>\n"
	"    <dc:publisher>O'Reilly Media, Inc.</dc:publisher>\n"
	"    <dc:date>2005-01-19</dc:date>\n"
	"    <dc:type>Python and XML</dc:type>\n"
	"    <dc:format>text/html</dc:format>\n"
	"    <dc:language>en-us</dc:language>\n"
	"    <dc:rights>Copyright 2005, O'Reilly Media, Inc.</dc:rights>\n"
	"  </item>\n"
	"\n"
	"  <item rdf:about=\"http://www.xml.com/pub/a/2005/01/19/print.html\">\n"
	"    <title>Features: Printing XML: Why CSS Is Better than XSL</title>\n"
	"    <link>http://www.xml.com/pub/a/2005/01/19/print.html</link>\n"
	"    <description>\n"
	"    One of the old school debates among XML developers is &amp;quot;CSS versus XSLT.&amp;quot; H&amp;aring;kun Wium Lie and Michael Day revive that debate with a shot across XSL's bow.\n"
	"   </description>\n"
	"    <dc:source>XML.com</dc:source>\n"
	"    <dc:creator>Michael Day, H&amp;aring;kon Wium Lie</dc:creator>\n"
	"    <dc:subject>Style, Publishing</dc:subject>\n"
	"    <dc:publisher>O'Reilly Media, Inc.</dc:publisher>\n"
	"    <dc:date>2005-01-19</dc:date>\n"
	"    <dc:type>Features</dc:type>\n"
	"    <dc:format>text/html</dc:format>\n"
	"    <dc:language>en-us</dc:language>\n"
	"    <dc:rights>Copyright 2005, O'Reilly Media, Inc.</dc:rights>\n"
	"  </item>\n"
	"\n"
	"  <item rdf:about=\"http://www.xml.com/pub/a/2005/01/19/review.html\">\n"
	"    <title>Features: Reviewing the Architecture of the World Wide Web</title>\n"
	"    <link>http://www.xml.com/pub/a/2005/01/19/review.html</link>\n"
	"    <description>\n"
	"    Harry Halpin reviews the final published edition of the W3C TAG's Architecture of the World Wide Web document.\n"
	"   </description>\n"
	"    <dc:source>XML.com</dc:source>\n"
	"    <dc:creator>Harry Halpin</dc:creator>\n"
	"    <dc:subject>Web, Perspectives</dc:subject>\n"
	"    <dc:publisher>O'Reilly Media, Inc.</dc:publisher>\n"
	"    <dc:date>2005-01-19</dc:date>\n"
	"    <dc:type>Features</dc:type>\n"
	"    <dc:format>text/html</dc:format>\n"
	"    <dc:language>en-us</dc:language>\n"
	"    <dc:rights>Copyright 2005, O'Reilly Media, Inc.</dc:rights>\n"
	"  </item>\n"
	"\n"
	"  <item rdf:about=\"http://www.xml.com/pub/a/2005/01/12/saml2.html\">\n"
	"    <title>Features: SAML 2: The Building Blocks of Federated Identity</title>\n"
	"    <link>http://www.xml.com/pub/a/2005/01/12/saml2.html</link>\n"
	"    <description>\n"
	"    Paul Madsen reports on the developments in web services security, including a new major release of SAML, which provides the basis for building federated identity.\n"
	"   </description>\n"
	"    <dc:source>XML.com</dc:source>\n"
	"    <dc:creator>Paul Madsen</dc:creator>\n"
	"    <dc:subject>Web Services, Specifications</dc:subject>\n"
	"    <dc:publisher>O'Reilly Media, Inc.</dc:publisher>\n"
	"    <dc:date>2005-01-12</dc:date>\n"
	"    <dc:type>Features</dc:type>\n"
	"    <dc:format>text/html</dc:format>\n"
	"    <dc:language>en-us</dc:language>\n"
	"    <dc:rights>Copyright 2005, O'Reilly Media, Inc.</dc:rights>\n"
	"  </item>\n"
	"\n"
	"  <item rdf:about=\"http://www.xml.com/pub/a/2005/01/12/comega.html\">\n"
	"    <title>Features: Introducing Comega</title>\n"
	"    <link>http://www.xml.com/pub/a/2005/01/12/comega.html</link>\n"
	"    <description>\n"
	"    Dare Obasanjo explains some of the ways in which C&amp;omega;--a new language from Microsoft Research--makes XML processing easier and more natural.\n"
	"   </description>\n"
	"    <dc:source>XML.com</dc:source>\n"
	"    <dc:creator>Dare Obasanjo</dc:creator>\n"
	"    <dc:subject>Programming, Instruction</dc:subject>\n"
	"    <dc:publisher>O'Reilly Media, Inc.</dc:publisher>\n"
	"    <dc:date>2005-01-12</dc:date>\n"
	"    <dc:type>Features</dc:type>\n"
	"    <dc:format>text/html</dc:format>\n"
	"    <dc:language>en-us</dc:language>\n"
	"    <dc:rights>Copyright 2005, O'Reilly Media, Inc.</dc:rights>\n"
	"  </item>\n"
	"\n"
	"\n"
	"  <textinput rdf:about=\"http://meerkat.oreillynet.com/\">\n"
	"    <title>Search</title>\n"
	"    <description>Search Meerkat...</description>\n"
	"    <name>s</name>\n"
	"    <link>http://meerkat.oreillynet.com/</link>\n"
	"  </textinput>\n"
	"\n"
	"</rdf:RDF>\n";


const std::string SAXParserTest::ENCODING =
	"<?xml version=\"1.0\" encoding=\"ISO-8859-15\"?>"
	"<euro-sign>\244</euro-sign>";

const std::string SAXParserTest::WSDL =
	"<!-- WSDL description of the Google Web APIs.\n"
	"     The Google Web APIs are in beta release. All interfaces are subject to\n"
	"     change as we refine and extend our APIs. Please see the terms of use\n"
	"     for more information. -->\n"
	"<!-- Revision 2002-08-16 -->\n"
	"<ns1:definitions name=\"GoogleSearch\" targetNamespace=\"urn:GoogleSearch\" xmlns:ns1=\"http://schemas.xmlsoap.org/wsdl/\">\n"
	"\t<!-- Types for search - result elements, directory categories -->\n"
	"\t<ns1:types>\n"
	"\t\t<ns2:schema targetNamespace=\"urn:GoogleSearch\" xmlns:ns2=\"http://www.w3.org/2001/XMLSchema\">\n"
	"\t\t\t<ns2:complexType name=\"GoogleSearchResult\">\n"
	"\t\t\t\t<ns2:all>\n"
	"\t\t\t\t\t<ns2:element name=\"documentFiltering\" type=\"xsd:boolean\"/>\n"
	"\t\t\t\t\t<ns2:element name=\"searchComments\" type=\"xsd:string\"/>\n"
	"\t\t\t\t\t<ns2:element name=\"estimatedTotalResultsCount\" type=\"xsd:int\"/>\n"
	"\t\t\t\t\t<ns2:element name=\"estimateIsExact\" type=\"xsd:boolean\"/>\n"
	"\t\t\t\t\t<ns2:element name=\"resultElements\" type=\"typens:ResultElementArray\"/>\n"
	"\t\t\t\t\t<ns2:element name=\"searchQuery\" type=\"xsd:string\"/>\n"
	"\t\t\t\t\t<ns2:element name=\"startIndex\" type=\"xsd:int\"/>\n"
	"\t\t\t\t\t<ns2:element name=\"endIndex\" type=\"xsd:int\"/>\n"
	"\t\t\t\t\t<ns2:element name=\"searchTips\" type=\"xsd:string\"/>\n"
	"\t\t\t\t\t<ns2:element name=\"directoryCategories\" type=\"typens:DirectoryCategoryArray\"/>\n"
	"\t\t\t\t\t<ns2:element name=\"searchTime\" type=\"xsd:double\"/>\n"
	"\t\t\t\t</ns2:all>\n"
	"\t\t\t</ns2:complexType>\n"
	"\t\t\t<ns2:complexType name=\"ResultElement\">\n"
	"\t\t\t\t<ns2:all>\n"
	"\t\t\t\t\t<ns2:element name=\"summary\" type=\"xsd:string\"/>\n"
	"\t\t\t\t\t<ns2:element name=\"URL\" type=\"xsd:string\"/>\n"
	"\t\t\t\t\t<ns2:element name=\"snippet\" type=\"xsd:string\"/>\n"
	"\t\t\t\t\t<ns2:element name=\"title\" type=\"xsd:string\"/>\n"
	"\t\t\t\t\t<ns2:element name=\"cachedSize\" type=\"xsd:string\"/>\n"
	"\t\t\t\t\t<ns2:element name=\"relatedInformationPresent\" type=\"xsd:boolean\"/>\n"
	"\t\t\t\t\t<ns2:element name=\"hostName\" type=\"xsd:string\"/>\n"
	"\t\t\t\t\t<ns2:element name=\"directoryCategory\" type=\"typens:DirectoryCategory\"/>\n"
	"\t\t\t\t\t<ns2:element name=\"directoryTitle\" type=\"xsd:string\"/>\n"
	"\t\t\t\t</ns2:all>\n"
	"\t\t\t</ns2:complexType>\n"
	"\t\t\t<ns2:complexType name=\"ResultElementArray\">\n"
	"\t\t\t\t<ns2:complexContent>\n"
	"\t\t\t\t\t<ns2:restriction base=\"soapenc:Array\">\n"
	"\t\t\t\t\t\t<ns2:attribute ns1:arrayType=\"typens:ResultElement[]\" ref=\"soapenc:arrayType\"/>\n"
	"\t\t\t\t\t</ns2:restriction>\n"
	"\t\t\t\t</ns2:complexContent>\n"
	"\t\t\t</ns2:complexType>\n"
	"\t\t\t<ns2:complexType name=\"DirectoryCategoryArray\">\n"
	"\t\t\t\t<ns2:complexContent>\n"
	"\t\t\t\t\t<ns2:restriction base=\"soapenc:Array\">\n"
	"\t\t\t\t\t\t<ns2:attribute ns1:arrayType=\"typens:DirectoryCategory[]\" ref=\"soapenc:arrayType\"/>\n"
	"\t\t\t\t\t</ns2:restriction>\n"
	"\t\t\t\t</ns2:complexContent>\n"
	"\t\t\t</ns2:complexType>\n"
	"\t\t\t<ns2:complexType name=\"DirectoryCategory\">\n"
	"\t\t\t\t<ns2:all>\n"
	"\t\t\t\t\t<ns2:element name=\"fullViewableName\" type=\"xsd:string\"/>\n"
	"\t\t\t\t\t<ns2:element name=\"specialEncoding\" type=\"xsd:string\"/>\n"
	"\t\t\t\t</ns2:all>\n"
	"\t\t\t</ns2:complexType>\n"
	"\t\t</ns2:schema>\n"
	"\t</ns1:types>\n"
	"\t<!-- Messages for Google Web APIs - cached page, search, spelling. -->\n"
	"\t<ns1:message name=\"doGetCachedPage\">\n"
	"\t\t<ns1:part name=\"key\" type=\"xsd:string\"/>\n"
	"\t\t<ns1:part name=\"url\" type=\"xsd:string\"/>\n"
	"\t</ns1:message>\n"
	"\t<ns1:message name=\"doGetCachedPageResponse\">\n"
	"\t\t<ns1:part name=\"return\" type=\"xsd:base64Binary\"/>\n"
	"\t</ns1:message>\n"
	"\t<ns1:message name=\"doSpellingSuggestion\">\n"
	"\t\t<ns1:part name=\"key\" type=\"xsd:string\"/>\n"
	"\t\t<ns1:part name=\"phrase\" type=\"xsd:string\"/>\n"
	"\t</ns1:message>\n"
	"\t<ns1:message name=\"doSpellingSuggestionResponse\">\n"
	"\t\t<ns1:part name=\"return\" type=\"xsd:string\"/>\n"
	"\t</ns1:message>\n"
	"\t<!-- note, ie and oe are ignored by server; all traffic is UTF-8. -->\n"
	"\t<ns1:message name=\"doGoogleSearch\">\n"
	"\t\t<ns1:part name=\"key\" type=\"xsd:string\"/>\n"
	"\t\t<ns1:part name=\"q\" type=\"xsd:string\"/>\n"
	"\t\t<ns1:part name=\"start\" type=\"xsd:int\"/>\n"
	"\t\t<ns1:part name=\"maxResults\" type=\"xsd:int\"/>\n"
	"\t\t<ns1:part name=\"filter\" type=\"xsd:boolean\"/>\n"
	"\t\t<ns1:part name=\"restrict\" type=\"xsd:string\"/>\n"
	"\t\t<ns1:part name=\"safeSearch\" type=\"xsd:boolean\"/>\n"
	"\t\t<ns1:part name=\"lr\" type=\"xsd:string\"/>\n"
	"\t\t<ns1:part name=\"ie\" type=\"xsd:string\"/>\n"
	"\t\t<ns1:part name=\"oe\" type=\"xsd:string\"/>\n"
	"\t</ns1:message>\n"
	"\t<ns1:message name=\"doGoogleSearchResponse\">\n"
	"\t\t<ns1:part name=\"return\" type=\"typens:GoogleSearchResult\"/>\n"
	"\t</ns1:message>\n"
	"\t<!-- Port for Google Web APIs, \"GoogleSearch\" -->\n"
	"\t<ns1:portType name=\"GoogleSearchPort\">\n"
	"\t\t<ns1:operation name=\"doGetCachedPage\">\n"
	"\t\t\t<ns1:input message=\"typens:doGetCachedPage\"/>\n"
	"\t\t\t<ns1:output message=\"typens:doGetCachedPageResponse\"/>\n"
	"\t\t</ns1:operation>\n"
	"\t\t<ns1:operation name=\"doSpellingSuggestion\">\n"
	"\t\t\t<ns1:input message=\"typens:doSpellingSuggestion\"/>\n"
	"\t\t\t<ns1:output message=\"typens:doSpellingSuggestionResponse\"/>\n"
	"\t\t</ns1:operation>\n"
	"\t\t<ns1:operation name=\"doGoogleSearch\">\n"
	"\t\t\t<ns1:input message=\"typens:doGoogleSearch\"/>\n"
	"\t\t\t<ns1:output message=\"typens:doGoogleSearchResponse\"/>\n"
	"\t\t</ns1:operation>\n"
	"\t</ns1:portType>\n"
	"\t<!-- Binding for Google Web APIs - RPC, SOAP over HTTP -->\n"
	"\t<ns1:binding name=\"GoogleSearchBinding\" type=\"typens:GoogleSearchPort\">\n"
	"\t\t<ns3:binding style=\"rpc\" transport=\"http://schemas.xmlsoap.org/soap/http\" xmlns:ns3=\"http://schemas.xmlsoap.org/wsdl/soap/\"/>\n"
	"\t\t<ns1:operation name=\"doGetCachedPage\" xmlns:ns3=\"http://schemas.xmlsoap.org/wsdl/soap/\">\n"
	"\t\t\t<ns3:operation soapAction=\"urn:GoogleSearchAction\"/>\n"
	"\t\t\t<ns1:input>\n"
	"\t\t\t\t<ns3:body encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\" namespace=\"urn:GoogleSearch\" use=\"encoded\"/>\n"
	"\t\t\t</ns1:input>\n"
	"\t\t\t<ns1:output>\n"
	"\t\t\t\t<ns3:body encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\" namespace=\"urn:GoogleSearch\" use=\"encoded\"/>\n"
	"\t\t\t</ns1:output>\n"
	"\t\t</ns1:operation>\n"
	"\t\t<ns1:operation name=\"doSpellingSuggestion\" xmlns:ns3=\"http://schemas.xmlsoap.org/wsdl/soap/\">\n"
	"\t\t\t<ns3:operation soapAction=\"urn:GoogleSearchAction\"/>\n"
	"\t\t\t<ns1:input>\n"
	"\t\t\t\t<ns3:body encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\" namespace=\"urn:GoogleSearch\" use=\"encoded\"/>\n"
	"\t\t\t</ns1:input>\n"
	"\t\t\t<ns1:output>\n"
	"\t\t\t\t<ns3:body encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\" namespace=\"urn:GoogleSearch\" use=\"encoded\"/>\n"
	"\t\t\t</ns1:output>\n"
	"\t\t</ns1:operation>\n"
	"\t\t<ns1:operation name=\"doGoogleSearch\" xmlns:ns3=\"http://schemas.xmlsoap.org/wsdl/soap/\">\n"
	"\t\t\t<ns3:operation soapAction=\"urn:GoogleSearchAction\"/>\n"
	"\t\t\t<ns1:input>\n"
	"\t\t\t\t<ns3:body encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\" namespace=\"urn:GoogleSearch\" use=\"encoded\"/>\n"
	"\t\t\t</ns1:input>\n"
	"\t\t\t<ns1:output>\n"
	"\t\t\t\t<ns3:body encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\" namespace=\"urn:GoogleSearch\" use=\"encoded\"/>\n"
	"\t\t\t</ns1:output>\n"
	"\t\t</ns1:operation>\n"
	"\t</ns1:binding>\n"
	"\t<!-- Endpoint for Google Web APIs -->\n"
	"\t<ns1:service name=\"GoogleSearchService\">\n"
	"\t\t<ns1:port binding=\"typens:GoogleSearchBinding\" name=\"GoogleSearchPort\">\n"
	"\t\t\t<ns4:address location=\"http://api.google.com/search/beta2\" xmlns:ns4=\"http://schemas.xmlsoap.org/wsdl/soap/\"/>\n"
	"\t\t</ns1:port>\n"
	"\t</ns1:service>\n"
	"</ns1:definitions>\n";
