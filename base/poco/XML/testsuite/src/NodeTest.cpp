//
// NodeTest.cpp
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "NodeTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/DOM/Document.h"
#include "Poco/DOM/Element.h"
#include "Poco/DOM/DocumentFragment.h"
#include "Poco/DOM/AutoPtr.h"


using Poco::XML::Element;
using Poco::XML::Document;
using Poco::XML::DocumentFragment;
using Poco::XML::Node;
using Poco::XML::AutoPtr;


NodeTest::NodeTest(const std::string& name): CppUnit::TestCase(name)
{
}


NodeTest::~NodeTest()
{
}


void NodeTest::testInsert()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Element> pRoot = pDoc->createElement("root");

	assertTrue (!pRoot->hasChildNodes());
	assertTrue (pRoot->firstChild() == 0);
	assertTrue (pRoot->lastChild() == 0);

	AutoPtr<Element> pChild1 = pDoc->createElement("child1");
	pRoot->insertBefore(pChild1, 0);
	assertTrue (pRoot->hasChildNodes());
	assertTrue (pRoot->firstChild() == pChild1);
	assertTrue (pRoot->lastChild() == pChild1);
	
	assertTrue (pChild1->previousSibling() == 0);
	assertTrue (pChild1->nextSibling() == 0);
	
	AutoPtr<Element> pChild3 = pDoc->createElement("child3");
	pRoot->insertBefore(pChild3, 0);
	assertTrue (pRoot->firstChild() == pChild1);
	assertTrue (pRoot->lastChild() == pChild3);

	assertTrue (pChild1->previousSibling() == 0);
	assertTrue (pChild1->nextSibling() == pChild3);
	assertTrue (pChild3->previousSibling() == pChild1);
	assertTrue (pChild3->nextSibling() == 0);

	AutoPtr<Element> pChild0 = pDoc->createElement("child0");
	pRoot->insertBefore(pChild0, pChild1);
	assertTrue (pRoot->firstChild() == pChild0);
	assertTrue (pRoot->lastChild() == pChild3);
	
	assertTrue (pChild0->previousSibling() == 0);
	assertTrue (pChild0->nextSibling() == pChild1);
	assertTrue (pChild1->previousSibling() == pChild0);
	assertTrue (pChild1->nextSibling() == pChild3);
	assertTrue (pChild3->previousSibling() == pChild1);
	assertTrue (pChild3->nextSibling() == 0);
	
	AutoPtr<Element> pChild2 = pDoc->createElement("child2");
	pRoot->insertBefore(pChild2, pChild3);
	assertTrue (pRoot->firstChild() == pChild0);
	assertTrue (pRoot->lastChild() == pChild3);

	assertTrue (pChild0->previousSibling() == 0);
	assertTrue (pChild0->nextSibling() == pChild1);
	assertTrue (pChild1->previousSibling() == pChild0);
	assertTrue (pChild1->nextSibling() == pChild2);
	assertTrue (pChild2->previousSibling() == pChild1);
	assertTrue (pChild2->nextSibling() == pChild3);
	assertTrue (pChild3->previousSibling() == pChild2);
	assertTrue (pChild3->nextSibling() == 0);
}


void NodeTest::testAppend()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Element> pRoot = pDoc->createElement("root");

	AutoPtr<Element> pChild1 = pDoc->createElement("child1");
	pRoot->appendChild(pChild1);
	assertTrue (pRoot->firstChild() == pChild1);
	assertTrue (pRoot->lastChild() == pChild1);

	assertTrue (pChild1->previousSibling() == 0);
	assertTrue (pChild1->nextSibling() == 0);

	AutoPtr<Element> pChild2 = pDoc->createElement("child2");
	pRoot->appendChild(pChild2);
	assertTrue (pRoot->firstChild() == pChild1);
	assertTrue (pRoot->lastChild() == pChild2);

	assertTrue (pChild1->previousSibling() == 0);
	assertTrue (pChild1->nextSibling() == pChild2);
	assertTrue (pChild2->previousSibling() == pChild1);
	assertTrue (pChild2->nextSibling() == 0);

	AutoPtr<Element> pChild3 = pDoc->createElement("child3");
	pRoot->appendChild(pChild3);
	assertTrue (pRoot->firstChild() == pChild1);
	assertTrue (pRoot->lastChild() == pChild3);

	assertTrue (pChild1->previousSibling() == 0);
	assertTrue (pChild1->nextSibling() == pChild2);
	assertTrue (pChild2->previousSibling() == pChild1);
	assertTrue (pChild2->nextSibling() == pChild3);
	assertTrue (pChild3->previousSibling() == pChild2);
	assertTrue (pChild3->nextSibling() == 0);
}


void NodeTest::testRemove()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Element> pRoot = pDoc->createElement("root");

	AutoPtr<Element> pChild1 = pDoc->createElement("child1");
	pRoot->appendChild(pChild1);

	AutoPtr<Element> pChild2 = pDoc->createElement("child2");
	pRoot->appendChild(pChild2);

	AutoPtr<Element> pChild3 = pDoc->createElement("child3");
	pRoot->appendChild(pChild3);

	AutoPtr<Element> pChild4 = pDoc->createElement("child4");
	pRoot->appendChild(pChild4);
	
	pRoot->removeChild(pChild2);
	assertTrue (pChild2->previousSibling() == 0);
	assertTrue (pChild2->nextSibling() == 0);
	
	assertTrue (pChild1->previousSibling() == 0);
	assertTrue (pChild1->nextSibling() == pChild3);
	assertTrue (pChild3->previousSibling() == pChild1);
	assertTrue (pChild3->nextSibling() == pChild4);
	assertTrue (pChild4->previousSibling() == pChild3);
	assertTrue (pChild4->nextSibling() == 0);

	pRoot->removeChild(pChild4);
	assertTrue (pChild4->previousSibling() == 0);
	assertTrue (pChild4->nextSibling() == 0);
	
	assertTrue (pChild1->previousSibling() == 0);
	assertTrue (pChild1->nextSibling() == pChild3);
	assertTrue (pChild3->previousSibling() == pChild1);
	assertTrue (pChild3->nextSibling() == 0);

	assertTrue (pRoot->firstChild() == pChild1);
	assertTrue (pRoot->lastChild() == pChild3);

	pRoot->removeChild(pChild1);
	assertTrue (pChild1->previousSibling() == 0);
	assertTrue (pChild1->nextSibling() == 0);
	
	assertTrue (pRoot->firstChild() == pChild3);
	assertTrue (pRoot->lastChild() == pChild3);
	
	pRoot->removeChild(pChild3);
	assertTrue (pChild3->previousSibling() == 0);
	assertTrue (pChild3->nextSibling() == 0);
	assertTrue (pRoot->firstChild() == 0);
	assertTrue (pRoot->lastChild() == 0);
}


void NodeTest::testReplace()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Element> pRoot = pDoc->createElement("root");

	AutoPtr<Element> pChild1 = pDoc->createElement("child1");
	pRoot->appendChild(pChild1);

	AutoPtr<Element> pChild2 = pDoc->createElement("child2");
	pRoot->appendChild(pChild2);

	AutoPtr<Element> pChild3 = pDoc->createElement("child3");
	pRoot->appendChild(pChild3);

	AutoPtr<Element> pChild4 = pDoc->createElement("child4");
	pRoot->appendChild(pChild4);

	AutoPtr<Element> pChild11 = pDoc->createElement("child11");
	pRoot->replaceChild(pChild11, pChild1);
	
	assertTrue (pChild1->previousSibling() == 0);
	assertTrue (pChild1->nextSibling() == 0);
	assertTrue (pRoot->firstChild() == pChild11);
	assertTrue (pRoot->lastChild() == pChild4);
	assertTrue (pChild11->previousSibling() == 0);
	assertTrue (pChild11->nextSibling() == pChild2);
	assertTrue (pChild2->previousSibling() == pChild11);
	assertTrue (pChild2->nextSibling() == pChild3);
	assertTrue (pChild3->previousSibling() == pChild2);
	assertTrue (pChild3->nextSibling() == pChild4);
	assertTrue (pChild4->previousSibling() == pChild3);
	assertTrue (pChild4->nextSibling() == 0);

	AutoPtr<Element> pChild22 = pDoc->createElement("child22");
	pRoot->replaceChild(pChild22, pChild2);

	assertTrue (pChild2->previousSibling() == 0);
	assertTrue (pChild2->nextSibling() == 0);
	assertTrue (pRoot->firstChild() == pChild11);
	assertTrue (pRoot->lastChild() == pChild4);
	assertTrue (pChild11->previousSibling() == 0);
	assertTrue (pChild11->nextSibling() == pChild22);
	assertTrue (pChild22->previousSibling() == pChild11);
	assertTrue (pChild22->nextSibling() == pChild3);
	assertTrue (pChild3->previousSibling() == pChild22);
	assertTrue (pChild3->nextSibling() == pChild4);
	assertTrue (pChild4->previousSibling() == pChild3);
	assertTrue (pChild4->nextSibling() == 0);

	AutoPtr<Element> pChild33 = pDoc->createElement("child33");
	pRoot->replaceChild(pChild33, pChild3);

	assertTrue (pChild3->previousSibling() == 0);
	assertTrue (pChild3->nextSibling() == 0);
	assertTrue (pRoot->firstChild() == pChild11);
	assertTrue (pRoot->lastChild() == pChild4);
	assertTrue (pChild11->previousSibling() == 0);
	assertTrue (pChild11->nextSibling() == pChild22);
	assertTrue (pChild22->previousSibling() == pChild11);
	assertTrue (pChild22->nextSibling() == pChild33);
	assertTrue (pChild33->previousSibling() == pChild22);
	assertTrue (pChild33->nextSibling() == pChild4);
	assertTrue (pChild4->previousSibling() == pChild33);
	assertTrue (pChild4->nextSibling() == 0);

	AutoPtr<Element> pChild44 = pDoc->createElement("child44");
	pRoot->replaceChild(pChild44, pChild4);

	assertTrue (pChild4->previousSibling() == 0);
	assertTrue (pChild4->nextSibling() == 0);
	assertTrue (pRoot->firstChild() == pChild11);
	assertTrue (pRoot->lastChild() == pChild44);
	assertTrue (pChild11->previousSibling() == 0);
	assertTrue (pChild11->nextSibling() == pChild22);
	assertTrue (pChild22->previousSibling() == pChild11);
	assertTrue (pChild22->nextSibling() == pChild33);
	assertTrue (pChild33->previousSibling() == pChild22);
	assertTrue (pChild33->nextSibling() == pChild44);
	assertTrue (pChild44->previousSibling() == pChild33);
	assertTrue (pChild44->nextSibling() == 0);
}


void NodeTest::testInsertFragment1()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Element> pRoot = pDoc->createElement("root");
	AutoPtr<DocumentFragment> pFrag = pDoc->createDocumentFragment();

	assertTrue (!pRoot->hasChildNodes());
	assertTrue (pRoot->firstChild() == 0);
	assertTrue (pRoot->lastChild() == 0);

	AutoPtr<Element> pChild1 = pDoc->createElement("child1");
	pFrag->appendChild(pChild1);
	pRoot->insertBefore(pFrag, 0);
	assertTrue (pFrag->firstChild() == 0);
	assertTrue (pFrag->lastChild() == 0);
	assertTrue (pRoot->firstChild() == pChild1);
	assertTrue (pRoot->lastChild() == pChild1);
	
	assertTrue (pChild1->previousSibling() == 0);
	assertTrue (pChild1->nextSibling() == 0);
	
	AutoPtr<Element> pChild3 = pDoc->createElement("child3");
	pFrag->appendChild(pChild3);
	pRoot->insertBefore(pFrag, 0);
	assertTrue (pRoot->firstChild() == pChild1);
	assertTrue (pRoot->lastChild() == pChild3);

	assertTrue (pChild1->previousSibling() == 0);
	assertTrue (pChild1->nextSibling() == pChild3);
	assertTrue (pChild3->previousSibling() == pChild1);
	assertTrue (pChild3->nextSibling() == 0);

	AutoPtr<Element> pChild0 = pDoc->createElement("child0");
	pFrag->appendChild(pChild0);
	pRoot->insertBefore(pFrag, pChild1);
	assertTrue (pRoot->firstChild() == pChild0);
	assertTrue (pRoot->lastChild() == pChild3);
	
	assertTrue (pChild0->previousSibling() == 0);
	assertTrue (pChild0->nextSibling() == pChild1);
	assertTrue (pChild1->previousSibling() == pChild0);
	assertTrue (pChild1->nextSibling() == pChild3);
	assertTrue (pChild3->previousSibling() == pChild1);
	assertTrue (pChild3->nextSibling() == 0);
	
	AutoPtr<Element> pChild2 = pDoc->createElement("child2");
	pFrag->appendChild(pChild2);
	pRoot->insertBefore(pFrag, pChild3);
	assertTrue (pRoot->firstChild() == pChild0);
	assertTrue (pRoot->lastChild() == pChild3);

	assertTrue (pChild0->previousSibling() == 0);
	assertTrue (pChild0->nextSibling() == pChild1);
	assertTrue (pChild1->previousSibling() == pChild0);
	assertTrue (pChild1->nextSibling() == pChild2);
	assertTrue (pChild2->previousSibling() == pChild1);
	assertTrue (pChild2->nextSibling() == pChild3);
	assertTrue (pChild3->previousSibling() == pChild2);
	assertTrue (pChild3->nextSibling() == 0);
}


void NodeTest::testInsertFragment2()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Element> pRoot = pDoc->createElement("root");
	AutoPtr<DocumentFragment> pFrag = pDoc->createDocumentFragment();

	assertTrue (!pRoot->hasChildNodes());
	assertTrue (pRoot->firstChild() == 0);
	assertTrue (pRoot->lastChild() == 0);

	AutoPtr<Element> pChild2 = pDoc->createElement("child2");
	AutoPtr<Element> pChild3 = pDoc->createElement("child3");
	pFrag->appendChild(pChild2);
	pFrag->appendChild(pChild3);
	pRoot->insertBefore(pFrag, 0);
	assertTrue (pFrag->firstChild() == 0);
	assertTrue (pFrag->lastChild() == 0);
	assertTrue (pRoot->firstChild() == pChild2);
	assertTrue (pRoot->lastChild() == pChild3);
	
	assertTrue (pChild2->previousSibling() == 0);
	assertTrue (pChild2->nextSibling() == pChild3);
	assertTrue (pChild3->previousSibling() == pChild2);
	assertTrue (pChild3->nextSibling() == 0);
	
	AutoPtr<Element> pChild6 = pDoc->createElement("child6");
	AutoPtr<Element> pChild7 = pDoc->createElement("child7");
	pFrag->appendChild(pChild6);
	pFrag->appendChild(pChild7);
	pRoot->insertBefore(pFrag, 0);
	assertTrue (pRoot->firstChild() == pChild2);
	assertTrue (pRoot->lastChild() == pChild7);

	assertTrue (pChild2->previousSibling() == 0);
	assertTrue (pChild2->nextSibling() == pChild3);
	assertTrue (pChild3->previousSibling() == pChild2);
	assertTrue (pChild3->nextSibling() == pChild6);
	assertTrue (pChild6->previousSibling() == pChild3);
	assertTrue (pChild6->nextSibling() == pChild7);
	assertTrue (pChild7->previousSibling() == pChild6);
	assertTrue (pChild7->nextSibling() == 0);

	AutoPtr<Element> pChild0 = pDoc->createElement("child0");
	AutoPtr<Element> pChild1 = pDoc->createElement("child1");
	pFrag->appendChild(pChild0);
	pFrag->appendChild(pChild1);
	pRoot->insertBefore(pFrag, pChild2);
	assertTrue (pRoot->firstChild() == pChild0);
	assertTrue (pRoot->lastChild() == pChild7);
	
	assertTrue (pChild0->previousSibling() == 0);
	assertTrue (pChild0->nextSibling() == pChild1);
	assertTrue (pChild1->previousSibling() == pChild0);
	assertTrue (pChild1->nextSibling() == pChild2);
	assertTrue (pChild2->previousSibling() == pChild1);
	assertTrue (pChild2->nextSibling() == pChild3);
	assertTrue (pChild3->previousSibling() == pChild2);
	assertTrue (pChild3->nextSibling() == pChild6);
	assertTrue (pChild6->previousSibling() == pChild3);
	assertTrue (pChild6->nextSibling() == pChild7);
	assertTrue (pChild7->previousSibling() == pChild6);
	assertTrue (pChild7->nextSibling() == 0);
	
	AutoPtr<Element> pChild4 = pDoc->createElement("child4");
	AutoPtr<Element> pChild5 = pDoc->createElement("child5");
	pFrag->appendChild(pChild4);
	pFrag->appendChild(pChild5);
	pRoot->insertBefore(pFrag, pChild6);
	assertTrue (pRoot->firstChild() == pChild0);
	assertTrue (pRoot->lastChild() == pChild7);

	assertTrue (pChild0->previousSibling() == 0);
	assertTrue (pChild0->nextSibling() == pChild1);
	assertTrue (pChild1->previousSibling() == pChild0);
	assertTrue (pChild1->nextSibling() == pChild2);
	assertTrue (pChild2->previousSibling() == pChild1);
	assertTrue (pChild2->nextSibling() == pChild3);
	assertTrue (pChild3->previousSibling() == pChild2);
	assertTrue (pChild3->nextSibling() == pChild4);
	assertTrue (pChild4->previousSibling() == pChild3);
	assertTrue (pChild4->nextSibling() == pChild5);
	assertTrue (pChild5->previousSibling() == pChild4);
	assertTrue (pChild5->nextSibling() == pChild6);
	assertTrue (pChild6->previousSibling() == pChild5);
	assertTrue (pChild6->nextSibling() == pChild7);
	assertTrue (pChild7->previousSibling() == pChild6);
	assertTrue (pChild7->nextSibling() == 0);
}


void NodeTest::testInsertFragment3()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Element> pRoot = pDoc->createElement("root");
	AutoPtr<DocumentFragment> pFrag = pDoc->createDocumentFragment();

	assertTrue (!pRoot->hasChildNodes());
	assertTrue (pRoot->firstChild() == 0);
	assertTrue (pRoot->lastChild() == 0);

	AutoPtr<Element> pChild3 = pDoc->createElement("child3");
	AutoPtr<Element> pChild4 = pDoc->createElement("child4");
	AutoPtr<Element> pChild5 = pDoc->createElement("child5");
	pFrag->appendChild(pChild3);
	pFrag->appendChild(pChild4);
	pFrag->appendChild(pChild5);
	pRoot->insertBefore(pFrag, 0);
	assertTrue (pFrag->firstChild() == 0);
	assertTrue (pFrag->lastChild() == 0);
	assertTrue (pRoot->firstChild() == pChild3);
	assertTrue (pRoot->lastChild() == pChild5);
	
	assertTrue (pChild3->previousSibling() == 0);
	assertTrue (pChild3->nextSibling() == pChild4);
	assertTrue (pChild4->previousSibling() == pChild3);
	assertTrue (pChild4->nextSibling() == pChild5);
	assertTrue (pChild5->previousSibling() == pChild4);
	assertTrue (pChild5->nextSibling() == 0);
	
	AutoPtr<Element> pChild9  = pDoc->createElement("child9");
	AutoPtr<Element> pChild10 = pDoc->createElement("child10");
	AutoPtr<Element> pChild11 = pDoc->createElement("child11");
	pFrag->appendChild(pChild9);
	pFrag->appendChild(pChild10);
	pFrag->appendChild(pChild11);
	pRoot->insertBefore(pFrag, 0);
	assertTrue (pRoot->firstChild() == pChild3);
	assertTrue (pRoot->lastChild() == pChild11);

	assertTrue (pChild3->previousSibling() == 0);
	assertTrue (pChild3->nextSibling() == pChild4);
	assertTrue (pChild4->previousSibling() == pChild3);
	assertTrue (pChild4->nextSibling() == pChild5);
	assertTrue (pChild5->previousSibling() == pChild4);
	assertTrue (pChild5->nextSibling() == pChild9);
	assertTrue (pChild9->previousSibling() == pChild5);
	assertTrue (pChild9->nextSibling() == pChild10);
	assertTrue (pChild10->previousSibling() == pChild9);
	assertTrue (pChild10->nextSibling() == pChild11);
	assertTrue (pChild11->previousSibling() == pChild10);
	assertTrue (pChild11->nextSibling() == 0);

	AutoPtr<Element> pChild0 = pDoc->createElement("child0");
	AutoPtr<Element> pChild1 = pDoc->createElement("child1");
	AutoPtr<Element> pChild2 = pDoc->createElement("child2");
	pFrag->appendChild(pChild0);
	pFrag->appendChild(pChild1);
	pFrag->appendChild(pChild2);
	pRoot->insertBefore(pFrag, pChild3);
	assertTrue (pRoot->firstChild() == pChild0);
	assertTrue (pRoot->lastChild() == pChild11);
	
	assertTrue (pChild0->previousSibling() == 0);
	assertTrue (pChild0->nextSibling() == pChild1);
	assertTrue (pChild1->previousSibling() == pChild0);
	assertTrue (pChild1->nextSibling() == pChild2);
	assertTrue (pChild2->previousSibling() == pChild1);
	assertTrue (pChild2->nextSibling() == pChild3);
	assertTrue (pChild3->previousSibling() == pChild2);
	assertTrue (pChild3->nextSibling() == pChild4);
	assertTrue (pChild4->previousSibling() == pChild3);
	assertTrue (pChild4->nextSibling() == pChild5);
	assertTrue (pChild5->previousSibling() == pChild4);
	assertTrue (pChild5->nextSibling() == pChild9);
	assertTrue (pChild9->previousSibling() == pChild5);
	assertTrue (pChild9->nextSibling() == pChild10);
	assertTrue (pChild10->previousSibling() == pChild9);
	assertTrue (pChild10->nextSibling() == pChild11);
	assertTrue (pChild11->previousSibling() == pChild10);
	assertTrue (pChild11->nextSibling() == 0);
	
	AutoPtr<Element> pChild6 = pDoc->createElement("child6");
	AutoPtr<Element> pChild7 = pDoc->createElement("child7");
	AutoPtr<Element> pChild8 = pDoc->createElement("child8");
	pFrag->appendChild(pChild6);
	pFrag->appendChild(pChild7);
	pFrag->appendChild(pChild8);
	pRoot->insertBefore(pFrag, pChild9);
	assertTrue (pRoot->firstChild() == pChild0);
	assertTrue (pRoot->lastChild() == pChild11);

	assertTrue (pChild0->previousSibling() == 0);
	assertTrue (pChild0->nextSibling() == pChild1);
	assertTrue (pChild1->previousSibling() == pChild0);
	assertTrue (pChild1->nextSibling() == pChild2);
	assertTrue (pChild2->previousSibling() == pChild1);
	assertTrue (pChild2->nextSibling() == pChild3);
	assertTrue (pChild3->previousSibling() == pChild2);
	assertTrue (pChild3->nextSibling() == pChild4);
	assertTrue (pChild4->previousSibling() == pChild3);
	assertTrue (pChild4->nextSibling() == pChild5);
	assertTrue (pChild5->previousSibling() == pChild4);
	assertTrue (pChild5->nextSibling() == pChild6);
	assertTrue (pChild6->previousSibling() == pChild5);
	assertTrue (pChild6->nextSibling() == pChild7);
	assertTrue (pChild7->previousSibling() == pChild6);
	assertTrue (pChild7->nextSibling() == pChild8);
	assertTrue (pChild8->previousSibling() == pChild7);
	assertTrue (pChild8->nextSibling() == pChild9);
	assertTrue (pChild9->previousSibling() == pChild8);
	assertTrue (pChild9->nextSibling() == pChild10);
	assertTrue (pChild10->previousSibling() == pChild9);
	assertTrue (pChild10->nextSibling() == pChild11);
	assertTrue (pChild11->previousSibling() == pChild10);
	assertTrue (pChild11->nextSibling() == 0);
}


void NodeTest::testAppendFragment1()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Element> pRoot = pDoc->createElement("root");
	AutoPtr<DocumentFragment> pFrag = pDoc->createDocumentFragment();	

	AutoPtr<Element> pChild1 = pDoc->createElement("child1");
	pFrag->appendChild(pChild1);
	pRoot->appendChild(pFrag);
	assertTrue (pFrag->firstChild() == 0);
	assertTrue (pFrag->lastChild() == 0);
	assertTrue (pRoot->firstChild() == pChild1);
	assertTrue (pRoot->lastChild() == pChild1);

	assertTrue (pChild1->previousSibling() == 0);
	assertTrue (pChild1->nextSibling() == 0);

	AutoPtr<Element> pChild2 = pDoc->createElement("child2");
	pFrag->appendChild(pChild2);
	pRoot->appendChild(pFrag);
	assertTrue (pRoot->firstChild() == pChild1);
	assertTrue (pRoot->lastChild() == pChild2);

	assertTrue (pChild1->previousSibling() == 0);
	assertTrue (pChild1->nextSibling() == pChild2);
	assertTrue (pChild2->previousSibling() == pChild1);
	assertTrue (pChild2->nextSibling() == 0);

	AutoPtr<Element> pChild3 = pDoc->createElement("child3");
	pFrag->appendChild(pChild3);
	pRoot->appendChild(pFrag);
	assertTrue (pRoot->firstChild() == pChild1);
	assertTrue (pRoot->lastChild() == pChild3);

	assertTrue (pChild1->previousSibling() == 0);
	assertTrue (pChild1->nextSibling() == pChild2);
	assertTrue (pChild2->previousSibling() == pChild1);
	assertTrue (pChild2->nextSibling() == pChild3);
	assertTrue (pChild3->previousSibling() == pChild2);
	assertTrue (pChild3->nextSibling() == 0);
}


void NodeTest::testAppendFragment2()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Element> pRoot = pDoc->createElement("root");
	AutoPtr<DocumentFragment> pFrag = pDoc->createDocumentFragment();	

	AutoPtr<Element> pChild1 = pDoc->createElement("child1");
	AutoPtr<Element> pChild2 = pDoc->createElement("child2");
	pFrag->appendChild(pChild1);
	pFrag->appendChild(pChild2);
	pRoot->appendChild(pFrag);
	assertTrue (pFrag->firstChild() == 0);
	assertTrue (pFrag->lastChild() == 0);
	assertTrue (pRoot->firstChild() == pChild1);
	assertTrue (pRoot->lastChild() == pChild2);

	assertTrue (pChild1->previousSibling() == 0);
	assertTrue (pChild1->nextSibling() == pChild2);
	assertTrue (pChild2->previousSibling() == pChild1);
	assertTrue (pChild2->nextSibling() == 0);

	AutoPtr<Element> pChild3 = pDoc->createElement("child3");
	AutoPtr<Element> pChild4 = pDoc->createElement("child4");
	pFrag->appendChild(pChild3);
	pFrag->appendChild(pChild4);
	pRoot->appendChild(pFrag);
	assertTrue (pRoot->firstChild() == pChild1);
	assertTrue (pRoot->lastChild() == pChild4);

	assertTrue (pChild1->previousSibling() == 0);
	assertTrue (pChild1->nextSibling() == pChild2);
	assertTrue (pChild2->previousSibling() == pChild1);
	assertTrue (pChild2->nextSibling() == pChild3);
	assertTrue (pChild3->previousSibling() == pChild2);
	assertTrue (pChild3->nextSibling() == pChild4);
	assertTrue (pChild4->previousSibling() == pChild3);
	assertTrue (pChild4->nextSibling() == 0);

	AutoPtr<Element> pChild5 = pDoc->createElement("child5");
	AutoPtr<Element> pChild6 = pDoc->createElement("child6");
	pFrag->appendChild(pChild5);
	pFrag->appendChild(pChild6);
	pRoot->appendChild(pFrag);
	assertTrue (pRoot->firstChild() == pChild1);
	assertTrue (pRoot->lastChild() == pChild6);

	assertTrue (pChild1->previousSibling() == 0);
	assertTrue (pChild1->nextSibling() == pChild2);
	assertTrue (pChild2->previousSibling() == pChild1);
	assertTrue (pChild2->nextSibling() == pChild3);
	assertTrue (pChild3->previousSibling() == pChild2);
	assertTrue (pChild3->nextSibling() == pChild4);
	assertTrue (pChild4->previousSibling() == pChild3);
	assertTrue (pChild4->nextSibling() == pChild5);
	assertTrue (pChild5->previousSibling() == pChild4);
	assertTrue (pChild5->nextSibling() == pChild6);
	assertTrue (pChild6->previousSibling() == pChild5);
	assertTrue (pChild6->nextSibling() == 0);
}


void NodeTest::testAppendFragment3()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Element> pRoot = pDoc->createElement("root");
	AutoPtr<DocumentFragment> pFrag = pDoc->createDocumentFragment();	

	AutoPtr<Element> pChild1 = pDoc->createElement("child1");
	AutoPtr<Element> pChild2 = pDoc->createElement("child2");
	AutoPtr<Element> pChild3 = pDoc->createElement("child3");
	pFrag->appendChild(pChild1);
	pFrag->appendChild(pChild2);
	pFrag->appendChild(pChild3);
	pRoot->appendChild(pFrag);
	assertTrue (pFrag->firstChild() == 0);
	assertTrue (pFrag->lastChild() == 0);
	assertTrue (pRoot->firstChild() == pChild1);
	assertTrue (pRoot->lastChild() == pChild3);

	assertTrue (pChild1->previousSibling() == 0);
	assertTrue (pChild1->nextSibling() == pChild2);
	assertTrue (pChild2->previousSibling() == pChild1);
	assertTrue (pChild2->nextSibling() == pChild3);
	assertTrue (pChild3->previousSibling() == pChild2);
	assertTrue (pChild3->nextSibling() == 0);

	AutoPtr<Element> pChild4 = pDoc->createElement("child4");
	AutoPtr<Element> pChild5 = pDoc->createElement("child5");
	AutoPtr<Element> pChild6 = pDoc->createElement("child6");
	pFrag->appendChild(pChild4);
	pFrag->appendChild(pChild5);
	pFrag->appendChild(pChild6);
	pRoot->appendChild(pFrag);
	assertTrue (pRoot->firstChild() == pChild1);
	assertTrue (pRoot->lastChild() == pChild6);

	assertTrue (pChild1->previousSibling() == 0);
	assertTrue (pChild1->nextSibling() == pChild2);
	assertTrue (pChild2->previousSibling() == pChild1);
	assertTrue (pChild2->nextSibling() == pChild3);
	assertTrue (pChild3->previousSibling() == pChild2);
	assertTrue (pChild3->nextSibling() == pChild4);
	assertTrue (pChild4->previousSibling() == pChild3);
	assertTrue (pChild4->nextSibling() == pChild5);
	assertTrue (pChild5->previousSibling() == pChild4);
	assertTrue (pChild5->nextSibling() == pChild6);
	assertTrue (pChild6->previousSibling() == pChild5);
	assertTrue (pChild6->nextSibling() == 0);

	AutoPtr<Element> pChild7 = pDoc->createElement("child7");
	AutoPtr<Element> pChild8 = pDoc->createElement("child8");
	AutoPtr<Element> pChild9 = pDoc->createElement("child9");
	pFrag->appendChild(pChild7);
	pFrag->appendChild(pChild8);
	pFrag->appendChild(pChild9);
	pRoot->appendChild(pFrag);
	assertTrue (pRoot->firstChild() == pChild1);
	assertTrue (pRoot->lastChild() == pChild9);

	assertTrue (pChild1->previousSibling() == 0);
	assertTrue (pChild1->nextSibling() == pChild2);
	assertTrue (pChild2->previousSibling() == pChild1);
	assertTrue (pChild2->nextSibling() == pChild3);
	assertTrue (pChild3->previousSibling() == pChild2);
	assertTrue (pChild3->nextSibling() == pChild4);
	assertTrue (pChild4->previousSibling() == pChild3);
	assertTrue (pChild4->nextSibling() == pChild5);
	assertTrue (pChild5->previousSibling() == pChild4);
	assertTrue (pChild5->nextSibling() == pChild6);
	assertTrue (pChild6->previousSibling() == pChild5);
	assertTrue (pChild6->nextSibling() == pChild7);
	assertTrue (pChild7->previousSibling() == pChild6);
	assertTrue (pChild7->nextSibling() == pChild8);
	assertTrue (pChild8->previousSibling() == pChild7);
	assertTrue (pChild8->nextSibling() == pChild9);
	assertTrue (pChild9->previousSibling() == pChild8);
	assertTrue (pChild9->nextSibling() == 0);
}


void NodeTest::testReplaceFragment1()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Element> pRoot = pDoc->createElement("root");
	AutoPtr<DocumentFragment> pFrag = pDoc->createDocumentFragment();	

	AutoPtr<Element> pChild1 = pDoc->createElement("child1");
	pRoot->appendChild(pChild1);

	AutoPtr<Element> pChild2 = pDoc->createElement("child2");
	pRoot->appendChild(pChild2);

	AutoPtr<Element> pChild3 = pDoc->createElement("child3");
	pRoot->appendChild(pChild3);

	AutoPtr<Element> pChild4 = pDoc->createElement("child4");
	pRoot->appendChild(pChild4);

	AutoPtr<Element> pChild11 = pDoc->createElement("child11");
	pFrag->appendChild(pChild11);
	pRoot->replaceChild(pFrag, pChild1);
	assertTrue (pFrag->firstChild() == 0);
	assertTrue (pFrag->lastChild() == 0);
	
	assertTrue (pChild1->previousSibling() == 0);
	assertTrue (pChild1->nextSibling() == 0);
	assertTrue (pRoot->firstChild() == pChild11);
	assertTrue (pRoot->lastChild() == pChild4);
	assertTrue (pChild11->previousSibling() == 0);
	assertTrue (pChild11->nextSibling() == pChild2);
	assertTrue (pChild2->previousSibling() == pChild11);
	assertTrue (pChild2->nextSibling() == pChild3);
	assertTrue (pChild3->previousSibling() == pChild2);
	assertTrue (pChild3->nextSibling() == pChild4);
	assertTrue (pChild4->previousSibling() == pChild3);
	assertTrue (pChild4->nextSibling() == 0);

	AutoPtr<Element> pChild22 = pDoc->createElement("child22");
	pFrag->appendChild(pChild22);
	pRoot->replaceChild(pFrag, pChild2);

	assertTrue (pChild2->previousSibling() == 0);
	assertTrue (pChild2->nextSibling() == 0);
	assertTrue (pRoot->firstChild() == pChild11);
	assertTrue (pRoot->lastChild() == pChild4);
	assertTrue (pChild11->previousSibling() == 0);
	assertTrue (pChild11->nextSibling() == pChild22);
	assertTrue (pChild22->previousSibling() == pChild11);
	assertTrue (pChild22->nextSibling() == pChild3);
	assertTrue (pChild3->previousSibling() == pChild22);
	assertTrue (pChild3->nextSibling() == pChild4);
	assertTrue (pChild4->previousSibling() == pChild3);
	assertTrue (pChild4->nextSibling() == 0);

	AutoPtr<Element> pChild33 = pDoc->createElement("child33");
	pFrag->appendChild(pChild33);
	pRoot->replaceChild(pFrag, pChild3);

	assertTrue (pChild3->previousSibling() == 0);
	assertTrue (pChild3->nextSibling() == 0);
	assertTrue (pRoot->firstChild() == pChild11);
	assertTrue (pRoot->lastChild() == pChild4);
	assertTrue (pChild11->previousSibling() == 0);
	assertTrue (pChild11->nextSibling() == pChild22);
	assertTrue (pChild22->previousSibling() == pChild11);
	assertTrue (pChild22->nextSibling() == pChild33);
	assertTrue (pChild33->previousSibling() == pChild22);
	assertTrue (pChild33->nextSibling() == pChild4);
	assertTrue (pChild4->previousSibling() == pChild33);
	assertTrue (pChild4->nextSibling() == 0);

	AutoPtr<Element> pChild44 = pDoc->createElement("child44");
	pFrag->appendChild(pChild44);
	pRoot->replaceChild(pFrag, pChild4);

	assertTrue (pChild4->previousSibling() == 0);
	assertTrue (pChild4->nextSibling() == 0);
	assertTrue (pRoot->firstChild() == pChild11);
	assertTrue (pRoot->lastChild() == pChild44);
	assertTrue (pChild11->previousSibling() == 0);
	assertTrue (pChild11->nextSibling() == pChild22);
	assertTrue (pChild22->previousSibling() == pChild11);
	assertTrue (pChild22->nextSibling() == pChild33);
	assertTrue (pChild33->previousSibling() == pChild22);
	assertTrue (pChild33->nextSibling() == pChild44);
	assertTrue (pChild44->previousSibling() == pChild33);
	assertTrue (pChild44->nextSibling() == 0);
}


void NodeTest::testReplaceFragment2()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Element> pRoot = pDoc->createElement("root");
	AutoPtr<DocumentFragment> pFrag = pDoc->createDocumentFragment();	

	AutoPtr<Element> pChild1 = pDoc->createElement("child1");
	pRoot->appendChild(pChild1);

	AutoPtr<Element> pChild2 = pDoc->createElement("child2");
	pRoot->appendChild(pChild2);

	AutoPtr<Element> pChild3 = pDoc->createElement("child3");
	pRoot->appendChild(pChild3);

	AutoPtr<Element> pChild4 = pDoc->createElement("child4");
	pRoot->appendChild(pChild4);

	AutoPtr<Element> pChild11 = pDoc->createElement("child11");
	AutoPtr<Element> pChild12 = pDoc->createElement("child12");
	pFrag->appendChild(pChild11);
	pFrag->appendChild(pChild12);
	pRoot->replaceChild(pFrag, pChild1);
	assertTrue (pFrag->firstChild() == 0);
	assertTrue (pFrag->lastChild() == 0);
	
	assertTrue (pChild1->previousSibling() == 0);
	assertTrue (pChild1->nextSibling() == 0);
	assertTrue (pRoot->firstChild() == pChild11);
	assertTrue (pRoot->lastChild() == pChild4);
	assertTrue (pChild11->previousSibling() == 0);
	assertTrue (pChild11->nextSibling() == pChild12);
	assertTrue (pChild12->previousSibling() == pChild11);
	assertTrue (pChild12->nextSibling() == pChild2);
	assertTrue (pChild2->previousSibling() == pChild12);
	assertTrue (pChild2->nextSibling() == pChild3);
	assertTrue (pChild3->previousSibling() == pChild2);
	assertTrue (pChild3->nextSibling() == pChild4);
	assertTrue (pChild4->previousSibling() == pChild3);
	assertTrue (pChild4->nextSibling() == 0);

	AutoPtr<Element> pChild21 = pDoc->createElement("child21");
	AutoPtr<Element> pChild22 = pDoc->createElement("child22");
	pFrag->appendChild(pChild21);
	pFrag->appendChild(pChild22);
	pRoot->replaceChild(pFrag, pChild2);

	assertTrue (pChild2->previousSibling() == 0);
	assertTrue (pChild2->nextSibling() == 0);
	assertTrue (pRoot->firstChild() == pChild11);
	assertTrue (pRoot->lastChild() == pChild4);
	assertTrue (pChild11->previousSibling() == 0);
	assertTrue (pChild11->nextSibling() == pChild12);
	assertTrue (pChild12->previousSibling() == pChild11);
	assertTrue (pChild12->nextSibling() == pChild21);
	assertTrue (pChild21->previousSibling() == pChild12);
	assertTrue (pChild21->nextSibling() == pChild22);
	assertTrue (pChild22->previousSibling() == pChild21);
	assertTrue (pChild22->nextSibling() == pChild3);
	assertTrue (pChild3->previousSibling() == pChild22);
	assertTrue (pChild3->nextSibling() == pChild4);
	assertTrue (pChild4->previousSibling() == pChild3);
	assertTrue (pChild4->nextSibling() == 0);

	AutoPtr<Element> pChild31 = pDoc->createElement("child31");
	AutoPtr<Element> pChild32 = pDoc->createElement("child32");
	pFrag->appendChild(pChild31);
	pFrag->appendChild(pChild32);
	pRoot->replaceChild(pFrag, pChild3);

	assertTrue (pChild3->previousSibling() == 0);
	assertTrue (pChild3->nextSibling() == 0);
	assertTrue (pRoot->firstChild() == pChild11);
	assertTrue (pRoot->lastChild() == pChild4);
	assertTrue (pChild11->previousSibling() == 0);
	assertTrue (pChild11->nextSibling() == pChild12);
	assertTrue (pChild12->previousSibling() == pChild11);
	assertTrue (pChild12->nextSibling() == pChild21);
	assertTrue (pChild21->previousSibling() == pChild12);
	assertTrue (pChild21->nextSibling() == pChild22);
	assertTrue (pChild22->previousSibling() == pChild21);
	assertTrue (pChild22->nextSibling() == pChild31);
	assertTrue (pChild31->previousSibling() == pChild22);
	assertTrue (pChild31->nextSibling() == pChild32);
	assertTrue (pChild32->previousSibling() == pChild31);
	assertTrue (pChild32->nextSibling() == pChild4);
	assertTrue (pChild4->previousSibling() == pChild32);
	assertTrue (pChild4->nextSibling() == 0);

	AutoPtr<Element> pChild41 = pDoc->createElement("child41");
	AutoPtr<Element> pChild42 = pDoc->createElement("child42");
	pFrag->appendChild(pChild41);
	pFrag->appendChild(pChild42);
	pRoot->replaceChild(pFrag, pChild4);

	assertTrue (pChild4->previousSibling() == 0);
	assertTrue (pChild4->nextSibling() == 0);
	assertTrue (pRoot->firstChild() == pChild11);
	assertTrue (pRoot->lastChild() == pChild42);
	assertTrue (pChild11->previousSibling() == 0);
	assertTrue (pChild11->nextSibling() == pChild12);
	assertTrue (pChild12->previousSibling() == pChild11);
	assertTrue (pChild12->nextSibling() == pChild21);
	assertTrue (pChild21->previousSibling() == pChild12);
	assertTrue (pChild21->nextSibling() == pChild22);
	assertTrue (pChild22->previousSibling() == pChild21);
	assertTrue (pChild22->nextSibling() == pChild31);
	assertTrue (pChild31->previousSibling() == pChild22);
	assertTrue (pChild31->nextSibling() == pChild32);
	assertTrue (pChild32->previousSibling() == pChild31);
	assertTrue (pChild32->nextSibling() == pChild41);
	assertTrue (pChild41->previousSibling() == pChild32);
	assertTrue (pChild41->nextSibling() == pChild42);
	assertTrue (pChild42->previousSibling() == pChild41);
	assertTrue (pChild42->nextSibling() == 0);
}


void NodeTest::testReplaceFragment3()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Element> pRoot = pDoc->createElement("root");
	AutoPtr<DocumentFragment> pFrag = pDoc->createDocumentFragment();	

	AutoPtr<Element> pChild1 = pDoc->createElement("child1");
	pRoot->appendChild(pChild1);

	AutoPtr<Element> pChild2 = pDoc->createElement("child2");
	pRoot->appendChild(pChild2);

	AutoPtr<Element> pChild3 = pDoc->createElement("child3");
	pRoot->appendChild(pChild3);

	AutoPtr<Element> pChild4 = pDoc->createElement("child4");
	pRoot->appendChild(pChild4);

	AutoPtr<Element> pChild11 = pDoc->createElement("child11");
	AutoPtr<Element> pChild12 = pDoc->createElement("child12");
	AutoPtr<Element> pChild13 = pDoc->createElement("child13");
	pFrag->appendChild(pChild11);
	pFrag->appendChild(pChild12);
	pFrag->appendChild(pChild13);
	pRoot->replaceChild(pFrag, pChild1);
	assertTrue (pFrag->firstChild() == 0);
	assertTrue (pFrag->lastChild() == 0);
	
	assertTrue (pChild1->previousSibling() == 0);
	assertTrue (pChild1->nextSibling() == 0);
	assertTrue (pRoot->firstChild() == pChild11);
	assertTrue (pRoot->lastChild() == pChild4);
	assertTrue (pChild11->previousSibling() == 0);
	assertTrue (pChild11->nextSibling() == pChild12);
	assertTrue (pChild12->previousSibling() == pChild11);
	assertTrue (pChild12->nextSibling() == pChild13);
	assertTrue (pChild13->previousSibling() == pChild12);
	assertTrue (pChild13->nextSibling() == pChild2);
	assertTrue (pChild2->previousSibling() == pChild13);
	assertTrue (pChild2->nextSibling() == pChild3);
	assertTrue (pChild3->previousSibling() == pChild2);
	assertTrue (pChild3->nextSibling() == pChild4);
	assertTrue (pChild4->previousSibling() == pChild3);
	assertTrue (pChild4->nextSibling() == 0);

	AutoPtr<Element> pChild21 = pDoc->createElement("child21");
	AutoPtr<Element> pChild22 = pDoc->createElement("child22");
	AutoPtr<Element> pChild23 = pDoc->createElement("child23");
	pFrag->appendChild(pChild21);
	pFrag->appendChild(pChild22);
	pFrag->appendChild(pChild23);
	pRoot->replaceChild(pFrag, pChild2);

	assertTrue (pChild2->previousSibling() == 0);
	assertTrue (pChild2->nextSibling() == 0);
	assertTrue (pRoot->firstChild() == pChild11);
	assertTrue (pRoot->lastChild() == pChild4);
	assertTrue (pChild11->previousSibling() == 0);
	assertTrue (pChild11->nextSibling() == pChild12);
	assertTrue (pChild12->previousSibling() == pChild11);
	assertTrue (pChild12->nextSibling() == pChild13);
	assertTrue (pChild13->previousSibling() == pChild12);
	assertTrue (pChild13->nextSibling() == pChild21);
	assertTrue (pChild21->previousSibling() == pChild13);
	assertTrue (pChild21->nextSibling() == pChild22);
	assertTrue (pChild22->previousSibling() == pChild21);
	assertTrue (pChild22->nextSibling() == pChild23);
	assertTrue (pChild23->previousSibling() == pChild22);
	assertTrue (pChild23->nextSibling() == pChild3);
	assertTrue (pChild3->previousSibling() == pChild23);
	assertTrue (pChild3->nextSibling() == pChild4);
	assertTrue (pChild4->previousSibling() == pChild3);
	assertTrue (pChild4->nextSibling() == 0);

	AutoPtr<Element> pChild31 = pDoc->createElement("child31");
	AutoPtr<Element> pChild32 = pDoc->createElement("child32");
	AutoPtr<Element> pChild33 = pDoc->createElement("child33");
	pFrag->appendChild(pChild31);
	pFrag->appendChild(pChild32);
	pFrag->appendChild(pChild33);
	pRoot->replaceChild(pFrag, pChild3);

	assertTrue (pChild3->previousSibling() == 0);
	assertTrue (pChild3->nextSibling() == 0);
	assertTrue (pRoot->firstChild() == pChild11);
	assertTrue (pRoot->lastChild() == pChild4);
	assertTrue (pChild11->previousSibling() == 0);
	assertTrue (pChild11->nextSibling() == pChild12);
	assertTrue (pChild12->previousSibling() == pChild11);
	assertTrue (pChild12->nextSibling() == pChild13);
	assertTrue (pChild13->previousSibling() == pChild12);
	assertTrue (pChild13->nextSibling() == pChild21);
	assertTrue (pChild21->previousSibling() == pChild13);
	assertTrue (pChild21->nextSibling() == pChild22);
	assertTrue (pChild22->previousSibling() == pChild21);
	assertTrue (pChild22->nextSibling() == pChild23);
	assertTrue (pChild23->previousSibling() == pChild22);
	assertTrue (pChild23->nextSibling() == pChild31);
	assertTrue (pChild31->previousSibling() == pChild23);
	assertTrue (pChild31->nextSibling() == pChild32);
	assertTrue (pChild32->previousSibling() == pChild31);
	assertTrue (pChild32->nextSibling() == pChild33);
	assertTrue (pChild33->previousSibling() == pChild32);
	assertTrue (pChild33->nextSibling() == pChild4);
	assertTrue (pChild4->previousSibling() == pChild33);
	assertTrue (pChild4->nextSibling() == 0);

	AutoPtr<Element> pChild41 = pDoc->createElement("child41");
	AutoPtr<Element> pChild42 = pDoc->createElement("child42");
	AutoPtr<Element> pChild43 = pDoc->createElement("child43");
	pFrag->appendChild(pChild41);
	pFrag->appendChild(pChild42);
	pFrag->appendChild(pChild43);
	pRoot->replaceChild(pFrag, pChild4);

	assertTrue (pChild4->previousSibling() == 0);
	assertTrue (pChild4->nextSibling() == 0);
	assertTrue (pRoot->firstChild() == pChild11);
	assertTrue (pRoot->lastChild() == pChild43);
	assertTrue (pChild11->previousSibling() == 0);
	assertTrue (pChild11->nextSibling() == pChild12);
	assertTrue (pChild12->previousSibling() == pChild11);
	assertTrue (pChild12->nextSibling() == pChild13);
	assertTrue (pChild13->previousSibling() == pChild12);
	assertTrue (pChild13->nextSibling() == pChild21);
	assertTrue (pChild21->previousSibling() == pChild13);
	assertTrue (pChild21->nextSibling() == pChild22);
	assertTrue (pChild22->previousSibling() == pChild21);
	assertTrue (pChild22->nextSibling() == pChild23);
	assertTrue (pChild23->previousSibling() == pChild22);
	assertTrue (pChild23->nextSibling() == pChild31);
	assertTrue (pChild31->previousSibling() == pChild23);
	assertTrue (pChild31->nextSibling() == pChild32);
	assertTrue (pChild32->previousSibling() == pChild31);
	assertTrue (pChild32->nextSibling() == pChild33);
	assertTrue (pChild33->previousSibling() == pChild32);
	assertTrue (pChild33->nextSibling() == pChild41);
	assertTrue (pChild41->previousSibling() == pChild33);
	assertTrue (pChild41->nextSibling() == pChild42);
	assertTrue (pChild42->previousSibling() == pChild41);
	assertTrue (pChild42->nextSibling() == pChild43);
	assertTrue (pChild43->previousSibling() == pChild42);
	assertTrue (pChild43->nextSibling() == 0);
}


void NodeTest::setUp()
{
}


void NodeTest::tearDown()
{
}


CppUnit::Test* NodeTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("NodeTest");

	CppUnit_addTest(pSuite, NodeTest, testInsert);
	CppUnit_addTest(pSuite, NodeTest, testAppend);
	CppUnit_addTest(pSuite, NodeTest, testRemove);
	CppUnit_addTest(pSuite, NodeTest, testReplace);
	CppUnit_addTest(pSuite, NodeTest, testInsertFragment1);
	CppUnit_addTest(pSuite, NodeTest, testInsertFragment2);
	CppUnit_addTest(pSuite, NodeTest, testInsertFragment3);
	CppUnit_addTest(pSuite, NodeTest, testAppendFragment1);
	CppUnit_addTest(pSuite, NodeTest, testAppendFragment2);
	CppUnit_addTest(pSuite, NodeTest, testAppendFragment3);
	CppUnit_addTest(pSuite, NodeTest, testReplaceFragment1);
	CppUnit_addTest(pSuite, NodeTest, testReplaceFragment2);
	CppUnit_addTest(pSuite, NodeTest, testReplaceFragment3);

	return pSuite;
}
