//
// NodeAppenderTest.cpp
//
// $Id: //poco/1.4/XML/testsuite/src/NodeAppenderTest.cpp#1 $
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "NodeAppenderTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/DOM/NodeAppender.h"
#include "Poco/DOM/Element.h"
#include "Poco/DOM/Document.h"
#include "Poco/DOM/DocumentFragment.h"
#include "Poco/DOM/AutoPtr.h"


using Poco::XML::NodeAppender;
using Poco::XML::Element;
using Poco::XML::Document;
using Poco::XML::DocumentFragment;
using Poco::XML::AutoPtr;
using Poco::XML::XMLString;


NodeAppenderTest::NodeAppenderTest(const std::string& name): CppUnit::TestCase(name)
{
}


NodeAppenderTest::~NodeAppenderTest()
{
}


void NodeAppenderTest::testAppendNode()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Element>  pRoot = pDoc->createElement("root");
	pDoc->appendChild(pRoot);
	
	NodeAppender appender(pRoot);
	
	AutoPtr<Element> pElem1 = pDoc->createElement("elem");
	AutoPtr<Element> pElem2 = pDoc->createElement("elem");
	AutoPtr<Element> pElem3 = pDoc->createElement("elem");

	appender.appendChild(pElem1);
	appender.appendChild(pElem2);
	appender.appendChild(pElem3);

	assert (pRoot->firstChild() == pElem1);
	assert (pRoot->lastChild() == pElem3);
	
	assert (pElem1->nextSibling() == pElem2);
	assert (pElem2->nextSibling() == pElem3);
	assert (pElem3->nextSibling() == 0);
	
	assert (pElem1->previousSibling() == 0);
	assert (pElem2->previousSibling() == pElem1);	
	assert (pElem3->previousSibling() == pElem2);	
	
	assert (pElem1->parentNode() == pRoot);
	assert (pElem2->parentNode() == pRoot);
	assert (pElem3->parentNode() == pRoot);
}


void NodeAppenderTest::testAppendNodeList()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Element>  pRoot = pDoc->createElement("root");
	pDoc->appendChild(pRoot);
	
	NodeAppender appender(pRoot);
	
	AutoPtr<DocumentFragment> pFrag1 = pDoc->createDocumentFragment();
	AutoPtr<DocumentFragment> pFrag2 = pDoc->createDocumentFragment();
	AutoPtr<DocumentFragment> pFrag3 = pDoc->createDocumentFragment();
	
	AutoPtr<Element> pElem1 = pDoc->createElement("elem");
	AutoPtr<Element> pElem2 = pDoc->createElement("elem");
	AutoPtr<Element> pElem3 = pDoc->createElement("elem");
	AutoPtr<Element> pElem4 = pDoc->createElement("elem");

	pFrag2->appendChild(pElem1);
	pFrag2->appendChild(pElem2);
	pFrag2->appendChild(pElem3);
	
	pFrag3->appendChild(pElem4);
	
	appender.appendChild(pFrag1);
	assert (pRoot->firstChild() == 0);
	
	appender.appendChild(pFrag2);
	assert (pRoot->firstChild() == pElem1);
	assert (pRoot->lastChild() == pElem3);

	assert (pElem1->nextSibling() == pElem2);
	assert (pElem2->nextSibling() == pElem3);
	assert (pElem3->nextSibling() == 0);
	
	assert (pElem1->previousSibling() == 0);
	assert (pElem2->previousSibling() == pElem1);	
	assert (pElem3->previousSibling() == pElem2);	
	
	assert (pElem1->parentNode() == pRoot);
	assert (pElem2->parentNode() == pRoot);
	assert (pElem3->parentNode() == pRoot);
	
	appender.appendChild(pFrag3);
	assert (pRoot->lastChild() == pElem4);
	assert (pElem4->parentNode() == pRoot);
	assert (pElem3->nextSibling() == pElem4);
	assert (pElem4->previousSibling() == pElem3);
}


void NodeAppenderTest::setUp()
{
}


void NodeAppenderTest::tearDown()
{
}


CppUnit::Test* NodeAppenderTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("NodeAppenderTest");

	CppUnit_addTest(pSuite, NodeAppenderTest, testAppendNode);
	CppUnit_addTest(pSuite, NodeAppenderTest, testAppendNodeList);

	return pSuite;
}
