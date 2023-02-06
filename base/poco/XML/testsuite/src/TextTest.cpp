//
// TextTest.cpp
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "TextTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/DOM/Text.h"
#include "Poco/DOM/CDATASection.h"
#include "Poco/DOM/Element.h"
#include "Poco/DOM/Document.h"
#include "Poco/DOM/AutoPtr.h"


using Poco::XML::Text;
using Poco::XML::CDATASection;
using Poco::XML::Element;
using Poco::XML::Document;
using Poco::XML::AutoPtr;
using Poco::XML::XMLString;


TextTest::TextTest(const std::string& name): CppUnit::TestCase(name)
{
}


TextTest::~TextTest()
{
}


void TextTest::testLength()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Text> pText1 = pDoc->createTextNode("");
	assertTrue (pText1->length() == 0);
	AutoPtr<Text> pText2 = pDoc->createTextNode("foo bar");
	assertTrue (pText2->length() == 7);
}


void TextTest::testSubstring()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Text> pText1 = pDoc->createTextNode("foo bar");
	XMLString str = pText1->substringData(0, 3);
	assertTrue (str == "foo");
	str = pText1->substringData(4, 3);
	assertTrue (str == "bar");
	str = pText1->substringData(3, 0);
	assertTrue (str == "");
}


void TextTest::testAppend()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Text> pText1 = pDoc->createTextNode("foo");
	pText1->appendData("bar");
	assertTrue (pText1->data() == "foobar");
}


void TextTest::testInsert()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Text> pText1 = pDoc->createTextNode("bar");
	pText1->insertData(0, "foo");
	assertTrue (pText1->data() == "foobar");
	pText1->insertData(pText1->length(), "!");
	assertTrue (pText1->data() == "foobar!");
	pText1->insertData(3, " ");
	assertTrue (pText1->data() == "foo bar!");
}


void TextTest::testDelete()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Text> pText1 = pDoc->createTextNode("foo bar");
	pText1->deleteData(3, 1);
	assertTrue (pText1->data() == "foobar");
	pText1->deleteData(0, 3);
	assertTrue (pText1->data() == "bar");
	pText1->deleteData(1, 0);
	assertTrue (pText1->data() == "bar");
	pText1->deleteData(0, pText1->length());
	assertTrue (pText1->data() == "");
}


void TextTest::testReplace()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Text> pText1 = pDoc->createTextNode("foo bar");
	pText1->replaceData(0, 3, "FOO");
	assertTrue (pText1->data() == "FOO bar");
	pText1->replaceData(4, 3, "BAR!!!");
	assertTrue (pText1->data() == "FOO BAR!!!");
	pText1->replaceData(3, 1, "-");
	assertTrue (pText1->data() == "FOO-BAR!!!");
	pText1->replaceData(3, 1, "---");
	assertTrue (pText1->data() == "FOO---BAR!!!");
	pText1->replaceData(3, 3, " ");
	assertTrue (pText1->data() == "FOO BAR!!!");
	pText1->replaceData(0, pText1->length(), "foo bar");
	assertTrue (pText1->data() == "foo bar");
}


void TextTest::testSplit()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Element> pElem = pDoc->createElement("elem");
	AutoPtr<Text> pText1 = pDoc->createCDATASection("foobar");
	pElem->appendChild(pText1);
	pText1->splitText(3);
	assertTrue (pElem->firstChild()->nodeValue() == "foo");
	assertTrue (pElem->lastChild()->nodeValue() == "bar");
}


void TextTest::testSplitCDATA()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Element> pElem = pDoc->createElement("elem");
	AutoPtr<Text> pText1 = pDoc->createTextNode("foobar");
	pElem->appendChild(pText1);
	pText1->splitText(3);
	assertTrue (pElem->firstChild()->nodeValue() == "foo");
	assertTrue (pElem->lastChild()->nodeValue() == "bar");

}


void TextTest::setUp()
{
}


void TextTest::tearDown()
{
}


CppUnit::Test* TextTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("TextTest");

	CppUnit_addTest(pSuite, TextTest, testLength);
	CppUnit_addTest(pSuite, TextTest, testSubstring);
	CppUnit_addTest(pSuite, TextTest, testAppend);
	CppUnit_addTest(pSuite, TextTest, testInsert);
	CppUnit_addTest(pSuite, TextTest, testDelete);
	CppUnit_addTest(pSuite, TextTest, testReplace);
	CppUnit_addTest(pSuite, TextTest, testSplit);
	CppUnit_addTest(pSuite, TextTest, testSplitCDATA);

	return pSuite;
}
