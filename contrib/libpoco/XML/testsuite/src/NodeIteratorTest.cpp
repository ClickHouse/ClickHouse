//
// NodeIteratorTest.cpp
//
// $Id: //poco/1.4/XML/testsuite/src/NodeIteratorTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "NodeIteratorTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/DOM/NodeIterator.h"
#include "Poco/DOM/NodeFilter.h"
#include "Poco/DOM/Document.h"
#include "Poco/DOM/Element.h"
#include "Poco/DOM/Text.h"
#include "Poco/DOM/AutoPtr.h"


using Poco::XML::NodeIterator;
using Poco::XML::NodeFilter;
using Poco::XML::Element;
using Poco::XML::Document;
using Poco::XML::Text;
using Poco::XML::Node;
using Poco::XML::AutoPtr;
using Poco::XML::XMLString;


namespace
{
	class TestNodeFilter: public NodeFilter
	{
		short acceptNode(Node* node)
		{
			if (node->innerText() == "text1")
				return NodeFilter::FILTER_ACCEPT;
			else
				return NodeFilter::FILTER_REJECT;
		}
	};
}


NodeIteratorTest::NodeIteratorTest(const std::string& name): CppUnit::TestCase(name)
{
}


NodeIteratorTest::~NodeIteratorTest()
{
}


void NodeIteratorTest::testShowAll()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Element> pRoot = pDoc->createElement("root");
	AutoPtr<Element> pElem1 = pDoc->createElement("elem");
	AutoPtr<Element> pElem2 = pDoc->createElement("elem");
	AutoPtr<Text> pText1 = pDoc->createTextNode("text1");
	AutoPtr<Text> pText2 = pDoc->createTextNode("text2");
	
	pElem1->appendChild(pText1);
	pElem2->appendChild(pText2);
	pRoot->appendChild(pElem1);
	pRoot->appendChild(pElem2);
	pDoc->appendChild(pRoot);
	
	NodeIterator it(pRoot, NodeFilter::SHOW_ALL);
	
	assert (it.nextNode() == pRoot);
	assert (it.nextNode() == pElem1);
	assert (it.nextNode() == pText1);
	assert (it.nextNode() == pElem2);
	assert (it.nextNode() == pText2);
	assert (it.nextNode() == 0);
	
	assert (it.previousNode() == pText2);
	assert (it.previousNode() == pElem2);
	assert (it.previousNode() == pText1);
	assert (it.previousNode() == pElem1);
	assert (it.previousNode() == pRoot);
	assert (it.previousNode() == 0);
}


void NodeIteratorTest::testShowElements()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Element> pRoot = pDoc->createElement("root");
	AutoPtr<Element> pElem1 = pDoc->createElement("elem");
	AutoPtr<Element> pElem2 = pDoc->createElement("elem");
	AutoPtr<Text> pText1 = pDoc->createTextNode("text1");
	AutoPtr<Text> pText2 = pDoc->createTextNode("text2");
	
	pElem1->appendChild(pText1);
	pElem2->appendChild(pText2);
	pRoot->appendChild(pElem1);
	pRoot->appendChild(pElem2);
	pDoc->appendChild(pRoot);
	
	NodeIterator it(pRoot, NodeFilter::SHOW_ELEMENT);
	
	assert (it.nextNode() == pRoot);
	assert (it.nextNode() == pElem1);
	assert (it.nextNode() == pElem2);
	assert (it.nextNode() == 0);
	
	assert (it.previousNode() == pElem2);
	assert (it.previousNode() == pElem1);
	assert (it.previousNode() == pRoot);
	assert (it.previousNode() == 0);
}


void NodeIteratorTest::testFilter()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Element> pRoot = pDoc->createElement("root");
	AutoPtr<Element> pElem1 = pDoc->createElement("elem");
	AutoPtr<Element> pElem2 = pDoc->createElement("elem");
	AutoPtr<Text> pText1 = pDoc->createTextNode("text1");
	AutoPtr<Text> pText2 = pDoc->createTextNode("text2");
	
	pElem1->appendChild(pText1);
	pElem2->appendChild(pText2);
	pRoot->appendChild(pElem1);
	pRoot->appendChild(pElem2);
	pDoc->appendChild(pRoot);
	
	TestNodeFilter filter;
	NodeIterator it(pRoot, NodeFilter::SHOW_ELEMENT, &filter);
	
	assert (it.nextNode() == pElem1);
	assert (it.nextNode() == 0);
	
	assert (it.previousNode() == pElem1);
	assert (it.previousNode() == 0);
}


void NodeIteratorTest::testShowNothing()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Element> pRoot = pDoc->createElement("root");
	AutoPtr<Element> pElem1 = pDoc->createElement("elem");
	AutoPtr<Element> pElem2 = pDoc->createElement("elem");
	AutoPtr<Text> pText1 = pDoc->createTextNode("text1");
	AutoPtr<Text> pText2 = pDoc->createTextNode("text2");
	
	pElem1->appendChild(pText1);
	pElem2->appendChild(pText2);
	pRoot->appendChild(pElem1);
	pRoot->appendChild(pElem2);
	pDoc->appendChild(pRoot);
	
	NodeIterator it(pRoot, 0);
	
	assert (it.nextNode() == 0);
	
	assert (it.previousNode() == 0);
}


void NodeIteratorTest::setUp()
{
}


void NodeIteratorTest::tearDown()
{
}


CppUnit::Test* NodeIteratorTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("NodeIteratorTest");

	CppUnit_addTest(pSuite, NodeIteratorTest, testShowAll);
	CppUnit_addTest(pSuite, NodeIteratorTest, testShowElements);
	CppUnit_addTest(pSuite, NodeIteratorTest, testFilter);
	CppUnit_addTest(pSuite, NodeIteratorTest, testShowNothing);

	return pSuite;
}
