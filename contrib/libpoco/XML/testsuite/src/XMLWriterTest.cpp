//
// XMLWriterTest.cpp
//
// $Id: //poco/1.4/XML/testsuite/src/XMLWriterTest.cpp#4 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "XMLWriterTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/XML/XMLWriter.h"
#include "Poco/SAX/AttributesImpl.h"
#include "Poco/Exception.h"
#include <sstream>


using Poco::XML::XMLWriter;
using Poco::XML::AttributesImpl;


XMLWriterTest::XMLWriterTest(const std::string& name): CppUnit::TestCase(name)
{
}


XMLWriterTest::~XMLWriterTest()
{
}


void XMLWriterTest::testTrivial()
{
	std::ostringstream str;
	XMLWriter writer(str, 0);
	writer.startDocument();
	writer.startElement("", "", "foo");
	writer.endElement("", "", "foo");
	writer.endDocument();
	std::string xml = str.str();
	assert (xml == "<foo/>");
}


void XMLWriterTest::testTrivialCanonical()
{
	std::ostringstream str;
	XMLWriter writer(str, XMLWriter::CANONICAL_XML);
	writer.startDocument();
	writer.startElement("", "", "foo");
	writer.endElement("", "", "foo");
	writer.endDocument();
	std::string xml = str.str();
	assert (xml == "<foo></foo>");
}


void XMLWriterTest::testTrivialDecl()
{
	std::ostringstream str;
	XMLWriter writer(str, XMLWriter::WRITE_XML_DECLARATION);
	writer.startDocument();
	writer.startElement("", "", "foo");
	writer.endElement("", "", "foo");
	writer.endDocument();
	std::string xml = str.str();
	assert (xml == "<?xml version=\"1.0\" encoding=\"UTF-8\"?><foo/>");
}


void XMLWriterTest::testTrivialDeclPretty()
{
	std::ostringstream str;
	XMLWriter writer(str, XMLWriter::WRITE_XML_DECLARATION | XMLWriter::PRETTY_PRINT);
	writer.setNewLine("\n");
	writer.startDocument();
	writer.startElement("", "", "foo");
	writer.endElement("", "", "foo");
	writer.endDocument();
	std::string xml = str.str();
	assert (xml == "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<foo/>\n");
}


void XMLWriterTest::testTrivialFragment()
{
	std::ostringstream str;
	XMLWriter writer(str, 0);
	writer.startFragment();
	writer.startElement("", "", "foo");
	writer.endElement("", "", "foo");
	writer.endFragment();
	std::string xml = str.str();
	assert (xml == "<foo/>");
}


void XMLWriterTest::testTrivialFragmentPretty()
{
	std::ostringstream str;
	XMLWriter writer(str, XMLWriter::WRITE_XML_DECLARATION | XMLWriter::PRETTY_PRINT);
	writer.setNewLine("\n");
	writer.startFragment();
	writer.startElement("", "", "foo");
	writer.endElement("", "", "foo");
	writer.endFragment();
	std::string xml = str.str();
	assert (xml == "<foo/>\n");
}


void XMLWriterTest::testDTDPretty()
{
	std::ostringstream str;
	XMLWriter writer(str, XMLWriter::WRITE_XML_DECLARATION | XMLWriter::PRETTY_PRINT);
	writer.setNewLine("\n");
	writer.startDocument();
	writer.startDTD("test", "", "http://www.appinf.com/DTDs/test");
	writer.endDTD();
	writer.startElement("", "", "foo");
	writer.endElement("", "", "foo");
	writer.endDocument();
	std::string xml = str.str();
	assert (xml == "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
	               "<!DOCTYPE test SYSTEM \"http://www.appinf.com/DTDs/test\">\n"
	               "<foo/>\n");
}


void XMLWriterTest::testDTD()
{
	std::ostringstream str;
	XMLWriter writer(str, XMLWriter::WRITE_XML_DECLARATION);
	writer.setNewLine("\n");
	writer.startDocument();
	writer.startDTD("test", "", "http://www.appinf.com/DTDs/test");
	writer.endDTD();
	writer.startElement("", "", "foo");
	writer.endElement("", "", "foo");
	writer.endDocument();
	std::string xml = str.str();
	assert (xml == "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
	               "<!DOCTYPE test SYSTEM \"http://www.appinf.com/DTDs/test\">"
	               "<foo/>");
}


void XMLWriterTest::testDTDPublic()
{
	std::ostringstream str;
	XMLWriter writer(str, XMLWriter::WRITE_XML_DECLARATION);
	writer.setNewLine("\n");
	writer.startDocument();
	writer.startDTD("test", "test", "http://www.appinf.com/DTDs/test");
	writer.endDTD();
	writer.startElement("", "", "foo");
	writer.endElement("", "", "foo");
	writer.endDocument();
	std::string xml = str.str();
	assert (xml == "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
	               "<!DOCTYPE test PUBLIC \"test\" \"http://www.appinf.com/DTDs/test\">"
	               "<foo/>");
}


void XMLWriterTest::testDTDNotation()
{
	std::ostringstream str;
	XMLWriter writer(str, XMLWriter::WRITE_XML_DECLARATION | XMLWriter::PRETTY_PRINT);
	writer.setNewLine("\n");
	writer.startDocument();
	writer.startDTD("test", "", "");
	std::string systemId("quicktime");
	writer.notationDecl("mov", 0, &systemId);
	std::string publicId("-//W3C//NOTATION XML 1.0//EN");
	writer.notationDecl("xml", &publicId, 0);
	writer.endDTD();
	writer.startElement("", "", "foo");
	writer.endElement("", "", "foo");
	writer.endDocument();
	std::string xml = str.str();
	assert (xml == "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
	               "<!DOCTYPE test [\n"
	               "\t<!NOTATION mov SYSTEM \"quicktime\">\n"
	               "\t<!NOTATION xml PUBLIC \"-//W3C//NOTATION XML 1.0//EN\">\n"
	               "]>\n"
	               "<foo/>\n");
}


void XMLWriterTest::testDTDEntity()
{
	std::ostringstream str;
	XMLWriter writer(str, XMLWriter::WRITE_XML_DECLARATION | XMLWriter::PRETTY_PRINT);
	writer.setNewLine("\n");
	writer.startDocument();
	writer.startDTD("test", "", "");
	std::string systemId("quicktime");
	writer.notationDecl("mov", 0, &systemId);
	std::string publicId("-//W3C//NOTATION XML 1.0//EN");
	writer.unparsedEntityDecl("movie", 0, "movie.mov", "mov");
	writer.endDTD();
	writer.startElement("", "", "foo");
	writer.endElement("", "", "foo");
	writer.endDocument();
	std::string xml = str.str();
	assert (xml == "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
	               "<!DOCTYPE test [\n"
	               "\t<!NOTATION mov SYSTEM \"quicktime\">\n"
	               "\t<!ENTITY movie SYSTEM \"movie.mov\" NDATA mov>\n"
	               "]>\n"
	               "<foo/>\n");
}


void XMLWriterTest::testAttributes()
{
	std::ostringstream str;
	XMLWriter writer(str, 0);
	writer.startDocument();
	AttributesImpl attrs;
	attrs.addAttribute("", "", "a1", "CDATA", "v1");
	attrs.addAttribute("", "", "a2", "CDATA", "v2");
	writer.startElement("", "", "el", attrs);
	writer.endElement("", "", "el");
	writer.endDocument();
	std::string xml = str.str();
	assert (xml == "<el a1=\"v1\" a2=\"v2\"/>");
}


void XMLWriterTest::testAttributesPretty()
{
	std::ostringstream str;
	XMLWriter writer(str, XMLWriter::PRETTY_PRINT | XMLWriter::PRETTY_PRINT_ATTRIBUTES);
	writer.setNewLine(XMLWriter::NEWLINE_LF);
	writer.startDocument();
	AttributesImpl attrs;
	attrs.addAttribute("", "", "a1", "CDATA", "v1");
	attrs.addAttribute("", "", "a2", "CDATA", "v2");
	writer.startElement("", "", "el", attrs);
	writer.endElement("", "", "el");
	writer.endDocument();
	std::string xml = str.str();
	assert (xml == "<el\n\ta1=\"v1\"\n\ta2=\"v2\"/>\n");
}


void XMLWriterTest::testData()
{
	std::ostringstream str;
	XMLWriter writer(str, 0);
	writer.startDocument();
	writer.dataElement("", "", "d", "data", "a1", "v1", "a2", "v2", "a3", "v3");
	writer.endDocument();
	std::string xml = str.str();
	assert (xml == "<d a1=\"v1\" a2=\"v2\" a3=\"v3\">data</d>");
}


void XMLWriterTest::testEmptyData()
{
	std::ostringstream str;
	XMLWriter writer(str, 0);
	writer.startDocument();
	writer.dataElement("", "", "d", "", "a1", "v1", "a2", "v2", "a3", "v3");
	writer.endDocument();
	std::string xml = str.str();
	assert (xml == "<d a1=\"v1\" a2=\"v2\" a3=\"v3\"/>");
}


void XMLWriterTest::testDataPretty()
{
	std::ostringstream str;
	XMLWriter writer(str, XMLWriter::PRETTY_PRINT);
	writer.setNewLine("\n");
	writer.startDocument();
	writer.startElement("", "", "r");
	writer.dataElement("", "", "d", "data", "a1", "v1", "a2", "v2", "a3", "v3");
	writer.endElement("", "", "r");
	writer.endDocument();
	std::string xml = str.str();
	assert (xml == "<r>\n\t<d a1=\"v1\" a2=\"v2\" a3=\"v3\">data</d>\n</r>\n");
}


void XMLWriterTest::testEmptyDataPretty()
{
	std::ostringstream str;
	XMLWriter writer(str, XMLWriter::PRETTY_PRINT);
	writer.setNewLine("\n");
	writer.startDocument();
	writer.startElement("", "", "r");
	writer.dataElement("", "", "d", "", "a1", "v1", "a2", "v2", "a3", "v3");
	writer.endElement("", "", "r");
	writer.endDocument();
	std::string xml = str.str();
	assert (xml == "<r>\n\t<d a1=\"v1\" a2=\"v2\" a3=\"v3\"/>\n</r>\n");
}


void XMLWriterTest::testComment()
{
	std::ostringstream str;
	XMLWriter writer(str, XMLWriter::PRETTY_PRINT);
	writer.setNewLine("\n");
	writer.startDocument();
	writer.comment("a comment", 0, 9);
	writer.startElement("", "", "r");
	writer.comment("<another comment>", 0, 17);
	writer.dataElement("", "", "d", "data");
	writer.endElement("", "", "r");
	writer.endDocument();
	std::string xml = str.str();
	assert (xml == "<!--a comment-->\n<r>\n\t<!--<another comment>-->\n\t<d>data</d>\n</r>\n");
}


void XMLWriterTest::testPI()
{
	std::ostringstream str;
	XMLWriter writer(str, XMLWriter::PRETTY_PRINT);
	writer.setNewLine("\n");
	writer.startDocument();
	writer.processingInstruction("target", "a processing instruction");
	writer.startElement("", "", "r");
	writer.processingInstruction("target", "another processing instruction");
	writer.dataElement("", "", "d", "data");
	writer.endElement("", "", "r");
	writer.endDocument();
	std::string xml = str.str();
	assert (xml == "<?target a processing instruction?>\n<r>\n\t<?target another processing instruction?>\n\t<d>data</d>\n</r>\n");
}


void XMLWriterTest::testCharacters()
{
	std::ostringstream str;
	XMLWriter writer(str, 0);
	writer.startDocument();
	writer.startElement("", "", "r");
	writer.characters("some \"chars\" that <must> be & escaped");
	writer.endElement("", "", "r");
	writer.endDocument();
	std::string xml = str.str();
	assert (xml == "<r>some &quot;chars&quot; that &lt;must&gt; be &amp; escaped</r>");
}


void XMLWriterTest::testEmptyCharacters()
{
	std::ostringstream str;
	XMLWriter writer(str, 0);
	writer.startDocument();
	writer.startElement("", "", "r");
	writer.characters("");
	writer.endElement("", "", "r");
	writer.endDocument();
	std::string xml = str.str();
	assert (xml == "<r/>");
}


void XMLWriterTest::testCDATA()
{
	std::ostringstream str;
	XMLWriter writer(str, 0);
	writer.startDocument();
	writer.startElement("", "", "r");
	writer.startCDATA();
	writer.characters("some \"chars\" that <must> be & escaped");
	writer.endCDATA();
	writer.endElement("", "", "r");
	writer.endDocument();
	std::string xml = str.str();
	assert (xml == "<r><![CDATA[some \"chars\" that <must> be & escaped]]></r>");
}


void XMLWriterTest::testRawCharacters()
{
	std::ostringstream str;
	XMLWriter writer(str, 0);
	writer.startDocument();
	writer.startElement("", "", "r");
	writer.startCDATA();
	writer.rawCharacters("some \"chars\" that <must> be & escaped");
	writer.endCDATA();
	writer.endElement("", "", "r");
	writer.endDocument();
	std::string xml = str.str();
	assert (xml == "<r><![CDATA[some \"chars\" that <must> be & escaped]]></r>");
}


void XMLWriterTest::testAttributeCharacters()
{
	std::ostringstream str;
	XMLWriter writer(str, 0);
	writer.startDocument();
	AttributesImpl attrs;
	attrs.addAttribute("", "", "a1", "CDATA", "a b c\n\td");
	attrs.addAttribute("", "", "a2", "CDATA", "a b c\r\nd");
	writer.startElement("", "", "el", attrs);
	writer.endElement("", "", "el");
	writer.endDocument();
	std::string xml = str.str();
	assert (xml == "<el a1=\"a b c&#xA;&#x9;d\" a2=\"a b c&#xD;&#xA;d\"/>");
}


void XMLWriterTest::testDefaultNamespace()
{
	std::ostringstream str;
	XMLWriter writer(str, 0);
	writer.startDocument();
	writer.startPrefixMapping("", "urn:ns");
	writer.startElement("", "", "r");
	writer.characters("data");
	writer.endElement("", "", "r");
	writer.endPrefixMapping("");
	writer.endDocument();
	std::string xml = str.str();
	assert (xml == "<r xmlns=\"urn:ns\">data</r>");
}


void XMLWriterTest::testQNamespaces()
{
	std::ostringstream str;
	XMLWriter writer(str, 0);
	writer.startDocument();
	writer.startElement("urn:ns", "r", "p:r");
	writer.characters("data");
	writer.endElement("urn:ns", "r", "p:r");
	writer.endDocument();
	std::string xml = str.str();
	assert (xml == "<p:r xmlns:p=\"urn:ns\">data</p:r>");
}


void XMLWriterTest::testQNamespacesNested()
{
	std::ostringstream str;
	XMLWriter writer(str, 0);
	writer.startDocument();
	writer.startElement("urn:ns", "r", "p:r");
	writer.startElement("urn:ns", "e", "p:e");
	writer.endElement("urn:ns", "e", "p:e");
	writer.endElement("urn:ns", "r", "p:r");
	writer.endDocument();
	std::string xml = str.str();
	assert (xml == "<p:r xmlns:p=\"urn:ns\"><p:e/></p:r>");
}


void XMLWriterTest::testNamespaces()
{
	std::ostringstream str;
	XMLWriter writer(str, 0);
	writer.startDocument();
	writer.startElement("urn:ns", "r", "");
	writer.characters("data");
	writer.endElement("urn:ns", "r", "");
	writer.endDocument();
	std::string xml = str.str();
	assert (xml == "<ns1:r xmlns:ns1=\"urn:ns\">data</ns1:r>");
}

void XMLWriterTest::testAttributeNamespaces()
{
	std::ostringstream str;
	XMLWriter writer(str, 0);
	Poco::XML::AttributesImpl attrs;
	attrs.addAttribute("urn:other", "myattr", "", "", "attrValue");
	attrs.addAttribute("urn:ns", "myattr2", "", "", "attrValue2");
	writer.startDocument();
	writer.startElement("urn:ns", "r", "", attrs);
	writer.characters("data");
	writer.endElement("urn:ns", "r", "");
	writer.endDocument();
	std::string xml = str.str();
	assert (xml == "<ns2:r ns1:myattr=\"attrValue\" ns2:myattr2=\"attrValue2\" xmlns:ns1=\"urn:other\" xmlns:ns2=\"urn:ns\">data</ns2:r>");
}


void XMLWriterTest::testNamespacesNested()
{
	std::ostringstream str;
	XMLWriter writer(str, 0);
	writer.startDocument();
	writer.startElement("urn:ns1", "r", "");
	writer.startElement("urn:ns1", "e", "");
	writer.endElement("urn:ns1", "e", "");
	writer.startElement("urn:ns2", "f", "");
	writer.endElement("urn:ns2", "f", "");
	writer.endElement("urn:ns1", "r", "");
	writer.endDocument();
	std::string xml = str.str();
	assert (xml == "<ns1:r xmlns:ns1=\"urn:ns1\"><ns1:e/><ns2:f xmlns:ns2=\"urn:ns2\"/></ns1:r>");
}


void XMLWriterTest::testExplicitNamespaces()
{
	std::ostringstream str;
	XMLWriter writer(str, 0);
	writer.startDocument();
	writer.startPrefixMapping("p1", "urn:ns1");
	writer.startPrefixMapping("p2", "urn:ns2");
	writer.startElement("urn:ns1", "r", "");
	writer.startElement("urn:ns2", "e", "");
	writer.endElement("urn:ns2", "e", "");
	writer.startPrefixMapping("p3", "urn:ns3");
	writer.startElement("urn:ns2", "e", "");
	writer.startElement("urn:ns3", "f", "");
	writer.endElement("urn:ns3", "f", "");
	writer.endElement("urn:ns2", "e", "");
	writer.endElement("urn:ns1", "r", "");
	writer.endDocument();
	std::string xml = str.str();
	assert (xml == "<p1:r xmlns:p1=\"urn:ns1\" xmlns:p2=\"urn:ns2\"><p2:e/><p2:e xmlns:p3=\"urn:ns3\"><p3:f/></p2:e></p1:r>");
}


void XMLWriterTest::testWellformed()
{
	std::ostringstream str;
	XMLWriter writer(str, 0);
	writer.startDocument();
	writer.startElement("", "", "foo");
	try
	{
		writer.endElement("", "", "bar");
		fail("not wellformed - must throw exception");
	}
	catch (Poco::Exception&)
	{
	}
}


void XMLWriterTest::testWellformedNested()
{
	std::ostringstream str;
	XMLWriter writer(str, 0);
	writer.startDocument();
	writer.startElement("", "", "foo");
	writer.startElement("", "", "bar");
	try
	{
		writer.endElement("", "", "foo");
		fail("not wellformed - must throw exception");
	}
	catch (Poco::Exception&)
	{
	}
}


void XMLWriterTest::testWellformedNamespace()
{
	std::ostringstream str;
	XMLWriter writer(str, 0);
	writer.startDocument();
	writer.startElement("urn:ns1", "foo", "");
	writer.startElement("urn:ns2", "bar", "");
	try
	{
		writer.endElement("urn:ns1", "bar", "");
		fail("not wellformed - must throw exception");
	}
	catch (Poco::Exception&)
	{
	}
}


void XMLWriterTest::testEmpty()
{
	std::ostringstream str;
	XMLWriter writer(str, 0);
	writer.startDocument();
	writer.startElement("", "", "foo");
	writer.startElement("", "", "bar");
	writer.emptyElement("", "", "empty");
	writer.endElement("", "", "bar");
	writer.endElement("", "", "foo");
	writer.endDocument();
	std::string xml = str.str();
	assert (xml == "<foo><bar><empty/></bar></foo>");
}


void XMLWriterTest::setUp()
{
}


void XMLWriterTest::tearDown()
{
}


CppUnit::Test* XMLWriterTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("XMLWriterTest");

	CppUnit_addTest(pSuite, XMLWriterTest, testTrivial);
	CppUnit_addTest(pSuite, XMLWriterTest, testTrivialCanonical);
	CppUnit_addTest(pSuite, XMLWriterTest, testTrivialDecl);
	CppUnit_addTest(pSuite, XMLWriterTest, testTrivialDeclPretty);
	CppUnit_addTest(pSuite, XMLWriterTest, testTrivialFragment);
	CppUnit_addTest(pSuite, XMLWriterTest, testTrivialFragmentPretty);
	CppUnit_addTest(pSuite, XMLWriterTest, testDTDPretty);
	CppUnit_addTest(pSuite, XMLWriterTest, testDTD);
	CppUnit_addTest(pSuite, XMLWriterTest, testDTDPublic);
	CppUnit_addTest(pSuite, XMLWriterTest, testDTDNotation);
	CppUnit_addTest(pSuite, XMLWriterTest, testDTDEntity);
	CppUnit_addTest(pSuite, XMLWriterTest, testAttributes);
	CppUnit_addTest(pSuite, XMLWriterTest, testAttributesPretty);
	CppUnit_addTest(pSuite, XMLWriterTest, testData);
	CppUnit_addTest(pSuite, XMLWriterTest, testEmptyData);
	CppUnit_addTest(pSuite, XMLWriterTest, testDataPretty);
	CppUnit_addTest(pSuite, XMLWriterTest, testEmptyDataPretty);
	CppUnit_addTest(pSuite, XMLWriterTest, testComment);
	CppUnit_addTest(pSuite, XMLWriterTest, testPI);
	CppUnit_addTest(pSuite, XMLWriterTest, testCharacters);
	CppUnit_addTest(pSuite, XMLWriterTest, testEmptyCharacters);
	CppUnit_addTest(pSuite, XMLWriterTest, testCDATA);
	CppUnit_addTest(pSuite, XMLWriterTest, testRawCharacters);
	CppUnit_addTest(pSuite, XMLWriterTest, testAttributeCharacters);
	CppUnit_addTest(pSuite, XMLWriterTest, testDefaultNamespace);
	CppUnit_addTest(pSuite, XMLWriterTest, testQNamespaces);
	CppUnit_addTest(pSuite, XMLWriterTest, testQNamespacesNested);
	CppUnit_addTest(pSuite, XMLWriterTest, testNamespaces);
	CppUnit_addTest(pSuite, XMLWriterTest, testAttributeNamespaces);
	CppUnit_addTest(pSuite, XMLWriterTest, testNamespacesNested);
	CppUnit_addTest(pSuite, XMLWriterTest, testExplicitNamespaces);
	CppUnit_addTest(pSuite, XMLWriterTest, testWellformed);
	CppUnit_addTest(pSuite, XMLWriterTest, testWellformedNested);
	CppUnit_addTest(pSuite, XMLWriterTest, testWellformedNamespace);
	CppUnit_addTest(pSuite, XMLWriterTest, testEmpty);

	return pSuite;
}
