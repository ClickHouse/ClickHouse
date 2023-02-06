//
// DocumentTest.cpp
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DocumentTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/DOM/Document.h"
#include "Poco/DOM/Element.h"
#include "Poco/DOM/Text.h"
#include "Poco/DOM/NodeList.h"
#include "Poco/DOM/AutoPtr.h"
#include "Poco/DOM/DOMException.h"


using Poco::XML::Element;
using Poco::XML::Document;
using Poco::XML::Text;
using Poco::XML::Node;
using Poco::XML::NodeList;
using Poco::XML::AutoPtr;
using Poco::XML::XMLString;
using Poco::XML::DOMException;


DocumentTest::DocumentTest(const std::string& name): CppUnit::TestCase(name)
{
}


DocumentTest::~DocumentTest()
{
}


void DocumentTest::testDocumentElement()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Element> pRoot = pDoc->createElement("root");

	assertTrue (pDoc->documentElement() == 0);
	pDoc->appendChild(pRoot);
	assertTrue (pDoc->documentElement() == pRoot);
	
	AutoPtr<Text> pText = pDoc->createTextNode("   ");
	pDoc->insertBefore(pText, pRoot);
	assertTrue (pDoc->documentElement() == pRoot);
	
}


void DocumentTest::testImport()
{
	AutoPtr<Document> pDoc1 = new Document;
	AutoPtr<Element> pRoot1 = pDoc1->createElement("root");

	AutoPtr<Document> pDoc2 = new Document;
	
	try
	{
		pDoc2->appendChild(pRoot1);
		fail("wrong document - must throw exception");
	}
	catch (DOMException&)
	{
	}
	
	AutoPtr<Element> pRoot2 = static_cast<Element*>(pDoc2->importNode(pRoot1, false));
	assertTrue (pRoot2->ownerDocument() == pDoc2);
	assertTrue (pRoot1->ownerDocument() == pDoc1);
	
	pDoc2->appendChild(pRoot2);
}


void DocumentTest::testImportDeep()
{
	AutoPtr<Document> pDoc1 = new Document;
	AutoPtr<Element> pRoot1 = pDoc1->createElement("root");
	AutoPtr<Element> pElem1 = pDoc1->createElement("elem");
	AutoPtr<Text> pText1 = pDoc1->createTextNode("text");

	pElem1->appendChild(pText1);
	pRoot1->appendChild(pElem1);

	pRoot1->setAttribute("a1", "v1");
	pRoot1->setAttribute("a2", "v2");

	AutoPtr<Document> pDoc2 = new Document;
	
	try
	{
		pDoc2->appendChild(pRoot1);
		fail("wrong document - must throw exception");
	}
	catch (DOMException&)
	{
	}
	
	AutoPtr<Element> pRoot2 = static_cast<Element*>(pDoc2->importNode(pRoot1, true));
	assertTrue (pRoot2->ownerDocument() == pDoc2);
	assertTrue (pRoot2->firstChild()->ownerDocument() == pDoc2);
	assertTrue (pRoot2->firstChild()->firstChild()->ownerDocument() == pDoc2);
	assertTrue (pRoot1->ownerDocument() == pDoc1);
	assertTrue (pRoot1->firstChild()->ownerDocument() == pDoc1);
	assertTrue (pRoot1->firstChild()->firstChild()->ownerDocument() == pDoc1);
	
	pDoc2->appendChild(pRoot2);
}


void DocumentTest::testElementsByTagName()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Element> pRoot = pDoc->createElement("root");
	pDoc->appendChild(pRoot);
	AutoPtr<NodeList> pNL1 = pDoc->getElementsByTagName("*");
	AutoPtr<NodeList> pNL2 = pDoc->getElementsByTagName("elem");
	
	assertTrue (pNL1->length() == 1);
	assertTrue (pNL1->item(0) == pRoot);
	assertTrue (pNL2->length() == 0);
	
	AutoPtr<Element> pElem1 = pDoc->createElement("elem");
	pRoot->appendChild(pElem1);
	
	assertTrue (pNL1->length() == 2);
	assertTrue (pNL2->length() == 1);
	assertTrue (pNL1->item(0) == pRoot);
	assertTrue (pNL1->item(1) == pElem1);
	assertTrue (pNL2->item(0) == pElem1);

	AutoPtr<Element> pElem2 = pDoc->createElement("Elem");
	pRoot->appendChild(pElem2);

	assertTrue (pNL1->length() == 3);
	assertTrue (pNL2->length() == 1);
	assertTrue (pNL1->item(0) == pRoot);
	assertTrue (pNL1->item(1) == pElem1);
	assertTrue (pNL1->item(2) == pElem2);
	assertTrue (pNL2->item(0) == pElem1);
}


void DocumentTest::testElementsByTagNameNS()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Element> pRoot = pDoc->createElementNS("urn:ns1", "root");
	pDoc->appendChild(pRoot);
	AutoPtr<NodeList> pNL1 = pDoc->getElementsByTagNameNS("*", "*");
	AutoPtr<NodeList> pNL2 = pDoc->getElementsByTagNameNS("*", "elem");
	
	assertTrue (pNL1->length() == 1);
	assertTrue (pNL1->item(0) == pRoot);
	assertTrue (pNL2->length() == 0);
	
	AutoPtr<Element> pElem1 = pDoc->createElementNS("urn:ns1", "elem");
	pRoot->appendChild(pElem1);
	
	assertTrue (pNL1->length() == 2);
	assertTrue (pNL2->length() == 1);
	assertTrue (pNL1->item(0) == pRoot);
	assertTrue (pNL1->item(1) == pElem1);
	assertTrue (pNL2->item(0) == pElem1);

	AutoPtr<Element> pElem2 = pDoc->createElementNS("urn:ns1", "Elem");
	pRoot->appendChild(pElem2);

	assertTrue (pNL1->length() == 3);
	assertTrue (pNL2->length() == 1);
	assertTrue (pNL1->item(0) == pRoot);
	assertTrue (pNL1->item(1) == pElem1);
	assertTrue (pNL1->item(2) == pElem2);
	assertTrue (pNL2->item(0) == pElem1);
}


void DocumentTest::testElementById()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Element> pRoot = pDoc->createElement("root");
	pRoot->setAttribute("id", "0");
	AutoPtr<Element> pElem1 = pDoc->createElement("elem");
	pElem1->setAttribute("id", "1");
	AutoPtr<Text> pText1 = pDoc->createTextNode("text");
	AutoPtr<Element> pElem2 = pDoc->createElement("elem");
	pElem2->setAttribute("id", "2");
	AutoPtr<Element> pElem3 = pDoc->createElement("elem");
	pElem3->setAttribute("id", "3");

	pElem1->appendChild(pText1);
	pElem1->appendChild(pElem2);
	pRoot->appendChild(pElem1);
	pRoot->appendChild(pElem3);
	pDoc->appendChild(pRoot);
	
	Element* pFound = pDoc->getElementById("0", "id");
	assertTrue (pFound == pRoot);
	
	pFound = pDoc->getElementById("1", "id");
	assertTrue (pFound == pElem1);
	
	pFound = pDoc->getElementById("2", "id");
	assertTrue (pFound == pElem2);

	pFound = pDoc->getElementById("3", "id");
	assertTrue (pFound == pElem3);

	pFound = pDoc->getElementById("4", "id");
	assertTrue (pFound == 0);
	
	pFound = pDoc->getElementById("0", "ID");
	assertTrue (pFound == 0);
}


void DocumentTest::testElementByIdNS()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Element> pRoot = pDoc->createElementNS("urn:ns1", "root");
	pRoot->setAttributeNS("urn:ns1", "id", "0");
	AutoPtr<Element> pElem1 = pDoc->createElementNS("urn:ns1", "elem");
	pElem1->setAttributeNS("urn:ns1", "id", "1");
	AutoPtr<Text> pText1 = pDoc->createTextNode("text");
	AutoPtr<Element> pElem2 = pDoc->createElementNS("urn:ns1", "elem");
	pElem2->setAttributeNS("urn:ns1", "id", "2");
	AutoPtr<Element> pElem3 = pDoc->createElementNS("urn:ns1", "elem");
	pElem3->setAttributeNS("urn:ns1", "id", "3");

	pElem1->appendChild(pText1);
	pElem1->appendChild(pElem2);
	pRoot->appendChild(pElem1);
	pRoot->appendChild(pElem3);
	pDoc->appendChild(pRoot);

	Element* pFound = pDoc->getElementByIdNS("0", "urn:ns1", "id");
	assertTrue (pFound == pRoot);
	
	pFound = pDoc->getElementByIdNS("1", "urn:ns1", "id");
	assertTrue (pFound == pElem1);
	
	pFound = pDoc->getElementByIdNS("2", "urn:ns1", "id");
	assertTrue (pFound == pElem2);

	pFound = pDoc->getElementByIdNS("3", "urn:ns1", "id");
	assertTrue (pFound == pElem3);

	pFound = pDoc->getElementByIdNS("4", "urn:ns1", "id");
	assertTrue (pFound == 0);
	
	pFound = pDoc->getElementByIdNS("0", "urn:ns1", "ID");
	assertTrue (pFound == 0);

	pFound = pDoc->getElementByIdNS("0", "urn:ns2", "id");
	assertTrue (pFound == 0);
}


void DocumentTest::setUp()
{
}


void DocumentTest::tearDown()
{
}


CppUnit::Test* DocumentTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("DocumentTest");

	CppUnit_addTest(pSuite, DocumentTest, testDocumentElement);
	CppUnit_addTest(pSuite, DocumentTest, testImport);
	CppUnit_addTest(pSuite, DocumentTest, testImportDeep);
	CppUnit_addTest(pSuite, DocumentTest, testElementsByTagName);
	CppUnit_addTest(pSuite, DocumentTest, testElementsByTagNameNS);
	CppUnit_addTest(pSuite, DocumentTest, testElementById);
	CppUnit_addTest(pSuite, DocumentTest, testElementByIdNS);

	return pSuite;
}
