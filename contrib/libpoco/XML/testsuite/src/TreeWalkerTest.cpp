//
// TreeWalkerTest.cpp
//
// $Id: //poco/1.4/XML/testsuite/src/TreeWalkerTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "TreeWalkerTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/DOM/TreeWalker.h"
#include "Poco/DOM/NodeFilter.h"
#include "Poco/DOM/Document.h"
#include "Poco/DOM/Element.h"
#include "Poco/DOM/Text.h"
#include "Poco/DOM/AutoPtr.h"


using Poco::XML::TreeWalker;
using Poco::XML::NodeFilter;
using Poco::XML::Element;
using Poco::XML::Document;
using Poco::XML::Text;
using Poco::XML::Node;
using Poco::XML::AutoPtr;
using Poco::XML::XMLString;


namespace
{
	class RejectNodeFilter: public NodeFilter
	{
		short acceptNode(Node* node)
		{
			if (node->nodeType() != Node::ELEMENT_NODE || node->innerText() == "text1" || node->nodeName() == "root")
				return NodeFilter::FILTER_ACCEPT;
			else
				return NodeFilter::FILTER_REJECT;
		}
	};

	class SkipNodeFilter: public NodeFilter
	{
		short acceptNode(Node* node)
		{
			if (node->nodeType() != Node::ELEMENT_NODE || node->innerText() == "text1")
				return NodeFilter::FILTER_ACCEPT;
			else
				return NodeFilter::FILTER_SKIP;
		}
	};
}


TreeWalkerTest::TreeWalkerTest(const std::string& name): CppUnit::TestCase(name)
{
}


TreeWalkerTest::~TreeWalkerTest()
{
}


void TreeWalkerTest::testShowAll()
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
	
	TreeWalker it(pRoot, NodeFilter::SHOW_ALL);
	
	assert (it.currentNode() == pRoot);
	assert (it.nextNode() == pElem1);
	assert (it.nextNode() == pText1);
	assert (it.nextNode() == pElem2);
	assert (it.nextNode() == pText2);
	assert (it.nextNode() == 0);
	
	assert (it.currentNode() == pText2);
	assert (it.previousNode() == pElem2);
	assert (it.previousNode() == pText1);
	assert (it.previousNode() == pElem1);
	assert (it.previousNode() == pRoot);
	assert (it.previousNode() == 0);
	
	assert (it.currentNode() == pRoot);
	assert (it.parentNode() == 0);
	assert (it.currentNode() == pRoot);
	assert (it.firstChild() == pElem1);
	assert (it.parentNode() == pRoot);
	assert (it.lastChild() == pElem2);
	assert (it.previousSibling() == pElem1);
	assert (it.previousSibling() == 0);
	assert (it.currentNode() == pElem1);
	assert (it.nextSibling() == pElem2);
	assert (it.nextSibling() == 0);
	assert (it.currentNode() == pElem2);
	assert (it.firstChild() == pText2);
	assert (it.nextSibling() == 0);
	assert (it.previousSibling() == 0);
	assert (it.parentNode() == pElem2);
	assert (it.lastChild() == pText2);
}


void TreeWalkerTest::testShowElements()
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
	
	TreeWalker it(pRoot, NodeFilter::SHOW_ELEMENT);
	
	assert (it.currentNode() == pRoot);
	assert (it.nextNode() == pElem1);
	assert (it.nextNode() == pElem2);
	assert (it.nextNode() == 0);
	
	assert (it.currentNode() == pElem2);
	assert (it.previousNode() == pElem1);
	assert (it.previousNode() == pRoot);
	assert (it.previousNode() == 0);
	
	assert (it.currentNode() == pRoot);
	assert (it.parentNode() == 0);
	assert (it.currentNode() == pRoot);
	assert (it.firstChild() == pElem1);
	assert (it.parentNode() == pRoot);
	assert (it.lastChild() == pElem2);
	assert (it.firstChild() == 0);
	assert (it.currentNode() == pElem2);
	assert (it.lastChild() == 0);
	assert (it.currentNode() == pElem2);
	assert (it.previousSibling() == pElem1);
	assert (it.firstChild() == 0);
	assert (it.lastChild() == 0);
	assert (it.parentNode() == pRoot);
}


void TreeWalkerTest::testFilter()
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
	
	SkipNodeFilter skipFilter;
	TreeWalker it1(pRoot, NodeFilter::SHOW_ELEMENT, &skipFilter);
	
	assert (it1.nextNode() == pElem1);
	assert (it1.nextNode() == 0);
	
	assert (it1.currentNode() == pElem1);
	assert (it1.previousNode() == 0);
	
	assert (it1.parentNode() == 0);
	assert (it1.firstChild() == 0);
	assert (it1.lastChild() == 0);
	assert (it1.nextSibling() == 0);
	assert (it1.previousSibling() == 0);

	TreeWalker it2(pRoot, NodeFilter::SHOW_ALL, &skipFilter);
	
	assert (it2.nextNode() == pElem1);
	assert (it2.nextNode() == pText1);
	assert (it2.nextNode() == pText2);
	assert (it2.nextNode() == 0);
	
	assert (it2.currentNode() == pText2);
	assert (it2.previousNode() == pText1);
	assert (it2.previousNode() == pElem1);
	assert (it2.previousNode() == 0);
	
	assert (it2.currentNode() == pElem1);
	assert (it2.parentNode() == 0);
	assert (it2.nextSibling() == 0);
	assert (it2.previousSibling() == 0);
	assert (it2.firstChild() == pText1);
	assert (it2.nextSibling() == 0);
	assert (it2.previousSibling() == 0);
	assert (it2.parentNode() == pElem1);

	RejectNodeFilter rejectFilter;
	TreeWalker it3(pRoot, NodeFilter::SHOW_ELEMENT, &rejectFilter);
	
	assert (it3.nextNode() == pElem1);
	assert (it3.nextNode() == 0);
	
	assert (it3.currentNode() == pElem1);
	assert (it3.previousNode() == pRoot);
	assert (it3.previousNode() == 0);
	
	assert (it3.currentNode() == pRoot);
	assert (it3.parentNode() == 0);
	assert (it3.firstChild() == pElem1);
	assert (it3.nextSibling() == 0);
	assert (it3.previousSibling() == 0);
	assert (it3.parentNode() == pRoot);
	assert (it3.lastChild() == pElem1);

	TreeWalker it4(pRoot, NodeFilter::SHOW_ALL, &rejectFilter);
	
	assert (it4.nextNode() == pElem1);
	assert (it4.nextNode() == pText1);
	assert (it4.nextNode() == 0);
	
	assert (it4.currentNode() == pText1);
	assert (it4.previousNode() == pElem1);
	assert (it4.previousNode() == pRoot);
	assert (it4.previousNode() == 0);
	
	assert (it4.currentNode() == pRoot);
	assert (it4.parentNode() == 0);
	assert (it4.firstChild() == pElem1);
	assert (it4.firstChild() == pText1);
	assert (it4.nextSibling() == 0);
	assert (it4.previousSibling() == 0);
	assert (it4.parentNode() == pElem1);
	assert (it4.lastChild() == pText1);
	assert (it4.parentNode() == pElem1);
	assert (it4.nextSibling() == 0);
	assert (it4.previousSibling() == 0);
	assert (it4.parentNode() == pRoot);
}


void TreeWalkerTest::testShowNothing()
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
	
	TreeWalker it(pRoot, 0);
	
	assert (it.nextNode() == 0);
	
	assert (it.previousNode() == 0);
	
	assert (it.currentNode() == pRoot);
	assert (it.firstChild() == 0);
	assert (it.lastChild() == 0);
	assert (it.nextSibling() == 0);
	assert (it.previousSibling() == 0);
}


void TreeWalkerTest::setUp()
{
}


void TreeWalkerTest::tearDown()
{
}


CppUnit::Test* TreeWalkerTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("TreeWalkerTest");

	CppUnit_addTest(pSuite, TreeWalkerTest, testShowAll);
	CppUnit_addTest(pSuite, TreeWalkerTest, testShowElements);
	CppUnit_addTest(pSuite, TreeWalkerTest, testFilter);
	CppUnit_addTest(pSuite, TreeWalkerTest, testShowNothing);

	return pSuite;
}
