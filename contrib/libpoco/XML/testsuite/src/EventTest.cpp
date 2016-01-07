//
// EventTest.cpp
//
// $Id: //poco/1.4/XML/testsuite/src/EventTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "EventTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/DOM/Event.h"
#include "Poco/DOM/MutationEvent.h"
#include "Poco/DOM/EventListener.h"
#include "Poco/DOM/Document.h"
#include "Poco/DOM/Element.h"
#include "Poco/DOM/Attr.h"
#include "Poco/DOM/Text.h"
#include "Poco/DOM/AutoPtr.h"


using Poco::XML::Event;
using Poco::XML::MutationEvent;
using Poco::XML::EventListener;
using Poco::XML::Element;
using Poco::XML::Document;
using Poco::XML::Attr;
using Poco::XML::Text;
using Poco::XML::Node;
using Poco::XML::AutoPtr;
using Poco::XML::XMLString;


class TestEventListener: public EventListener
{
public:
	TestEventListener(const XMLString& name, bool cancel = false, bool readd = false, bool capture = false):
		_name(name),
		_cancel(cancel),
		_readd(readd),
		_capture(capture)
	{
	}
	
	void handleEvent(Event* evt)
	{
		XMLString type = evt->type();
		XMLString phase;
		switch (evt->eventPhase())
		{
		case Event::CAPTURING_PHASE:
			phase = "CAPTURING_PHASE"; break;
		case Event::AT_TARGET:
			phase = "AT_TARGET"; break;
		case Event::BUBBLING_PHASE:
			phase = "BUBBLING_PHASE"; break;
		}
		Node* pTarget = static_cast<Node*>(evt->target());
		Node* pCurrentTarget = static_cast<Node*>(evt->currentTarget());
		
		_log.append(_name);
		_log.append(":");
		_log.append(type);
		_log.append(":");
		_log.append(phase);
		_log.append(":");
		_log.append(pTarget->nodeName());
		_log.append(":");
		_log.append(pCurrentTarget->nodeName());
		_log.append(":");
		_log.append(evt->bubbles() ? "B" : "-");
		_log.append(":");
		_log.append(evt->cancelable() ? "C" : "-");
		
		MutationEvent* pME = dynamic_cast<MutationEvent*>(evt);
		if (pME)
		{
			XMLString attrChange;
			switch (pME->attrChange())
			{
			case MutationEvent::MODIFICATION:
				attrChange = "MODIFICATION"; break;
			case MutationEvent::ADDITION:
				attrChange = "ADDITION"; break;
			case MutationEvent::REMOVAL:
				attrChange = "REMOVAL"; break;
			}
			XMLString relatedNode;
			Node* pRelatedNode = pME->relatedNode();
			if (pRelatedNode) relatedNode = pRelatedNode->nodeName();

			_log.append(":");
			_log.append(attrChange);
			_log.append(":");
			_log.append(relatedNode);
			_log.append(":");
			_log.append(pME->attrName());
			_log.append(":");
			_log.append(pME->prevValue());
			_log.append(":");
			_log.append(pME->newValue());
		}
		_log.append("\n");
		
		if (_cancel) evt->stopPropagation();
		if (_readd)
			pCurrentTarget->addEventListener(type, this, _capture);
	}
	
	static const XMLString& log()
	{
		return _log;
	}
	
	static void reset()
	{
		_log.clear();
	}
	
private:
	XMLString _name;
	bool      _cancel;
	bool      _readd;
	bool      _capture;
	static XMLString _log;
};


XMLString TestEventListener::_log;


EventTest::EventTest(const std::string& name): CppUnit::TestCase(name)
{
}


EventTest::~EventTest()
{
}


void EventTest::testInsert()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Element> pRoot = pDoc->createElement("root");

	TestEventListener docListener("doc");
	TestEventListener docCapListener("docCap");
	TestEventListener rootListener("root");
	TestEventListener rootCapListener("rootCap");

	pDoc->addEventListener(MutationEvent::DOMSubtreeModified, &docListener, false);
	pDoc->addEventListener(MutationEvent::DOMNodeInserted, &docListener, false);
	pDoc->addEventListener(MutationEvent::DOMNodeInsertedIntoDocument, &docListener, false);

	pDoc->addEventListener(MutationEvent::DOMSubtreeModified, &docCapListener, true);
	pDoc->addEventListener(MutationEvent::DOMNodeInserted, &docCapListener, true);
	pDoc->addEventListener(MutationEvent::DOMNodeInsertedIntoDocument, &docCapListener, true);
	
	pRoot->addEventListener(MutationEvent::DOMSubtreeModified, &rootListener, false);
	pRoot->addEventListener(MutationEvent::DOMNodeInserted, &rootListener, false);
	pRoot->addEventListener(MutationEvent::DOMNodeInsertedIntoDocument, &rootListener, false);

	pRoot->addEventListener(MutationEvent::DOMSubtreeModified, &rootCapListener, true);
	pRoot->addEventListener(MutationEvent::DOMNodeInserted, &rootCapListener, true);
	pRoot->addEventListener(MutationEvent::DOMNodeInsertedIntoDocument, &rootCapListener, true);
	
	pDoc->appendChild(pRoot);
	
	const XMLString& log = TestEventListener::log();

	assert (log == 
		"docCap:DOMNodeInserted:CAPTURING_PHASE:root:#document:B:-:MODIFICATION:#document:::\n"
		"rootCap:DOMNodeInserted:AT_TARGET:root:root:B:-:MODIFICATION:#document:::\n"
		"root:DOMNodeInserted:AT_TARGET:root:root:B:-:MODIFICATION:#document:::\n"
		"doc:DOMNodeInserted:BUBBLING_PHASE:root:#document:B:-:MODIFICATION:#document:::\n"
		"docCap:DOMNodeInsertedIntoDocument:CAPTURING_PHASE:root:#document:-:-:MODIFICATION::::\n"
		"rootCap:DOMNodeInsertedIntoDocument:AT_TARGET:root:root:-:-:MODIFICATION::::\n"
		"root:DOMNodeInsertedIntoDocument:AT_TARGET:root:root:-:-:MODIFICATION::::\n"
		"docCap:DOMSubtreeModified:AT_TARGET:#document:#document:B:-:MODIFICATION::::\n"
		"doc:DOMSubtreeModified:AT_TARGET:#document:#document:B:-:MODIFICATION::::\n"
	);
	
	TestEventListener::reset();
	
	AutoPtr<Text> pText = pDoc->createTextNode("text");
	pRoot->appendChild(pText);

	assert (log == 
		"docCap:DOMNodeInserted:CAPTURING_PHASE:#text:#document:B:-:MODIFICATION:root:::\n"
		"rootCap:DOMNodeInserted:CAPTURING_PHASE:#text:root:B:-:MODIFICATION:root:::\n"
		"root:DOMNodeInserted:BUBBLING_PHASE:#text:root:B:-:MODIFICATION:root:::\n"
		"doc:DOMNodeInserted:BUBBLING_PHASE:#text:#document:B:-:MODIFICATION:root:::\n"
		"docCap:DOMNodeInsertedIntoDocument:CAPTURING_PHASE:#text:#document:-:-:MODIFICATION::::\n"
		"rootCap:DOMNodeInsertedIntoDocument:CAPTURING_PHASE:#text:root:-:-:MODIFICATION::::\n"
		"docCap:DOMSubtreeModified:CAPTURING_PHASE:root:#document:B:-:MODIFICATION::::\n"
		"rootCap:DOMSubtreeModified:AT_TARGET:root:root:B:-:MODIFICATION::::\n"
		"root:DOMSubtreeModified:AT_TARGET:root:root:B:-:MODIFICATION::::\n"
		"doc:DOMSubtreeModified:BUBBLING_PHASE:root:#document:B:-:MODIFICATION::::\n"
	);
}


void EventTest::testInsertSubtree()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Element> pRoot = pDoc->createElement("root");

	TestEventListener docListener("doc");
	TestEventListener docCapListener("docCap");
	TestEventListener rootListener("root");
	TestEventListener rootCapListener("rootCap");

	pDoc->addEventListener(MutationEvent::DOMSubtreeModified, &docListener, false);
	pDoc->addEventListener(MutationEvent::DOMNodeInserted, &docListener, false);
	pDoc->addEventListener(MutationEvent::DOMNodeInsertedIntoDocument, &docListener, false);

	pDoc->addEventListener(MutationEvent::DOMSubtreeModified, &docCapListener, true);
	pDoc->addEventListener(MutationEvent::DOMNodeInserted, &docCapListener, true);
	pDoc->addEventListener(MutationEvent::DOMNodeInsertedIntoDocument, &docCapListener, true);
	
	pRoot->addEventListener(MutationEvent::DOMSubtreeModified, &rootListener, false);
	pRoot->addEventListener(MutationEvent::DOMNodeInserted, &rootListener, false);
	pRoot->addEventListener(MutationEvent::DOMNodeInsertedIntoDocument, &rootListener, false);

	pRoot->addEventListener(MutationEvent::DOMSubtreeModified, &rootCapListener, true);
	pRoot->addEventListener(MutationEvent::DOMNodeInserted, &rootCapListener, true);
	pRoot->addEventListener(MutationEvent::DOMNodeInsertedIntoDocument, &rootCapListener, true);
	
	AutoPtr<Text> pText = pDoc->createTextNode("text");
	pRoot->appendChild(pText);

	TestEventListener::reset();

	pDoc->appendChild(pRoot);

	const XMLString& log = TestEventListener::log();
	assert (log == 
		"docCap:DOMNodeInserted:CAPTURING_PHASE:root:#document:B:-:MODIFICATION:#document:::\n"
		"rootCap:DOMNodeInserted:AT_TARGET:root:root:B:-:MODIFICATION:#document:::\n"
		"root:DOMNodeInserted:AT_TARGET:root:root:B:-:MODIFICATION:#document:::\n"
		"doc:DOMNodeInserted:BUBBLING_PHASE:root:#document:B:-:MODIFICATION:#document:::\n"
		"docCap:DOMNodeInsertedIntoDocument:CAPTURING_PHASE:root:#document:-:-:MODIFICATION::::\n"
		"rootCap:DOMNodeInsertedIntoDocument:AT_TARGET:root:root:-:-:MODIFICATION::::\n"
		"root:DOMNodeInsertedIntoDocument:AT_TARGET:root:root:-:-:MODIFICATION::::\n"
		"docCap:DOMNodeInsertedIntoDocument:CAPTURING_PHASE:#text:#document:-:-:MODIFICATION::::\n"
		"rootCap:DOMNodeInsertedIntoDocument:CAPTURING_PHASE:#text:root:-:-:MODIFICATION::::\n"
		"docCap:DOMSubtreeModified:AT_TARGET:#document:#document:B:-:MODIFICATION::::\n"
		"doc:DOMSubtreeModified:AT_TARGET:#document:#document:B:-:MODIFICATION::::\n"
	);
}


void EventTest::testRemove()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Element> pRoot = pDoc->createElement("root");

	TestEventListener docListener("doc");
	TestEventListener docCapListener("docCap");
	TestEventListener rootListener("root");
	TestEventListener rootCapListener("rootCap");

	pDoc->addEventListener(MutationEvent::DOMSubtreeModified, &docListener, false);
	pDoc->addEventListener(MutationEvent::DOMNodeRemoved, &docListener, false);
	pDoc->addEventListener(MutationEvent::DOMNodeRemovedFromDocument, &docListener, false);

	pDoc->addEventListener(MutationEvent::DOMSubtreeModified, &docCapListener, true);
	pDoc->addEventListener(MutationEvent::DOMNodeRemoved, &docCapListener, true);
	pDoc->addEventListener(MutationEvent::DOMNodeRemovedFromDocument, &docCapListener, true);
	
	pRoot->addEventListener(MutationEvent::DOMSubtreeModified, &rootListener, false);
	pRoot->addEventListener(MutationEvent::DOMNodeRemoved, &rootListener, false);
	pRoot->addEventListener(MutationEvent::DOMNodeRemovedFromDocument, &rootListener, false);

	pRoot->addEventListener(MutationEvent::DOMSubtreeModified, &rootCapListener, true);
	pRoot->addEventListener(MutationEvent::DOMNodeRemoved, &rootCapListener, true);
	pRoot->addEventListener(MutationEvent::DOMNodeRemovedFromDocument, &rootCapListener, true);
	
	pDoc->appendChild(pRoot);
	
	AutoPtr<Text> pText = pDoc->createTextNode("text");
	pRoot->appendChild(pText);

	TestEventListener::reset();

	pRoot->removeChild(pText);

	const XMLString& log = TestEventListener::log();
	assert (log == 
		"docCap:DOMNodeRemoved:CAPTURING_PHASE:#text:#document:B:-:MODIFICATION:root:::\n"
		"rootCap:DOMNodeRemoved:CAPTURING_PHASE:#text:root:B:-:MODIFICATION:root:::\n"
		"root:DOMNodeRemoved:BUBBLING_PHASE:#text:root:B:-:MODIFICATION:root:::\n"
		"doc:DOMNodeRemoved:BUBBLING_PHASE:#text:#document:B:-:MODIFICATION:root:::\n"
		"docCap:DOMNodeRemovedFromDocument:CAPTURING_PHASE:#text:#document:-:-:MODIFICATION::::\n"
		"rootCap:DOMNodeRemovedFromDocument:CAPTURING_PHASE:#text:root:-:-:MODIFICATION::::\n"
		"docCap:DOMSubtreeModified:CAPTURING_PHASE:root:#document:B:-:MODIFICATION::::\n"
		"rootCap:DOMSubtreeModified:AT_TARGET:root:root:B:-:MODIFICATION::::\n"
		"root:DOMSubtreeModified:AT_TARGET:root:root:B:-:MODIFICATION::::\n"
		"doc:DOMSubtreeModified:BUBBLING_PHASE:root:#document:B:-:MODIFICATION::::\n"
	);
}


void EventTest::testRemoveSubtree()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Element> pRoot = pDoc->createElement("root");

	TestEventListener docListener("doc");
	TestEventListener docCapListener("docCap");
	TestEventListener rootListener("root");
	TestEventListener rootCapListener("rootCap");

	pDoc->addEventListener(MutationEvent::DOMSubtreeModified, &docListener, false);
	pDoc->addEventListener(MutationEvent::DOMNodeRemoved, &docListener, false);
	pDoc->addEventListener(MutationEvent::DOMNodeRemovedFromDocument, &docListener, false);

	pDoc->addEventListener(MutationEvent::DOMSubtreeModified, &docCapListener, true);
	pDoc->addEventListener(MutationEvent::DOMNodeRemoved, &docCapListener, true);
	pDoc->addEventListener(MutationEvent::DOMNodeRemovedFromDocument, &docCapListener, true);
	
	pRoot->addEventListener(MutationEvent::DOMSubtreeModified, &rootListener, false);
	pRoot->addEventListener(MutationEvent::DOMNodeRemoved, &rootListener, false);
	pRoot->addEventListener(MutationEvent::DOMNodeRemovedFromDocument, &rootListener, false);

	pRoot->addEventListener(MutationEvent::DOMSubtreeModified, &rootCapListener, true);
	pRoot->addEventListener(MutationEvent::DOMNodeRemoved, &rootCapListener, true);
	pRoot->addEventListener(MutationEvent::DOMNodeRemovedFromDocument, &rootCapListener, true);
	
	pDoc->appendChild(pRoot);
	
	AutoPtr<Text> pText = pDoc->createTextNode("text");
	pRoot->appendChild(pText);

	TestEventListener::reset();

	pDoc->removeChild(pRoot);

	const XMLString& log = TestEventListener::log();	
	assert (log == 
		"docCap:DOMNodeRemoved:CAPTURING_PHASE:root:#document:B:-:MODIFICATION:#document:::\n"
		"rootCap:DOMNodeRemoved:AT_TARGET:root:root:B:-:MODIFICATION:#document:::\n"
		"root:DOMNodeRemoved:AT_TARGET:root:root:B:-:MODIFICATION:#document:::\n"
		"doc:DOMNodeRemoved:BUBBLING_PHASE:root:#document:B:-:MODIFICATION:#document:::\n"
		"docCap:DOMNodeRemovedFromDocument:CAPTURING_PHASE:root:#document:-:-:MODIFICATION::::\n"
		"rootCap:DOMNodeRemovedFromDocument:AT_TARGET:root:root:-:-:MODIFICATION::::\n"
		"root:DOMNodeRemovedFromDocument:AT_TARGET:root:root:-:-:MODIFICATION::::\n"
		"docCap:DOMNodeRemovedFromDocument:CAPTURING_PHASE:#text:#document:-:-:MODIFICATION::::\n"
		"rootCap:DOMNodeRemovedFromDocument:CAPTURING_PHASE:#text:root:-:-:MODIFICATION::::\n"
		"docCap:DOMSubtreeModified:AT_TARGET:#document:#document:B:-:MODIFICATION::::\n"
		"doc:DOMSubtreeModified:AT_TARGET:#document:#document:B:-:MODIFICATION::::\n"
	);
}


void EventTest::testCharacterData()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Element> pRoot = pDoc->createElement("root");
	AutoPtr<Text> pText = pDoc->createTextNode("text");
	pRoot->appendChild(pText);
	pDoc->appendChild(pRoot);

	TestEventListener docListener("doc");
	TestEventListener docCapListener("docCap");
	TestEventListener rootListener("root");
	TestEventListener rootCapListener("rootCap");
	TestEventListener textListener("text");
	TestEventListener textCapListener("textCap");

	pDoc->addEventListener(MutationEvent::DOMCharacterDataModified, &docListener, false);
	pDoc->addEventListener(MutationEvent::DOMCharacterDataModified, &docCapListener, true);	
	pRoot->addEventListener(MutationEvent::DOMCharacterDataModified, &rootListener, false);
	pRoot->addEventListener(MutationEvent::DOMCharacterDataModified, &rootCapListener, true);
	pText->addEventListener(MutationEvent::DOMCharacterDataModified, &textListener, false);
	pText->addEventListener(MutationEvent::DOMCharacterDataModified, &textCapListener, true);

	TestEventListener::reset();
	
	pText->setData("modified");

	const XMLString& log = TestEventListener::log();	
	assert (log == 
		"docCap:DOMCharacterDataModified:CAPTURING_PHASE:#text:#document:B:-:MODIFICATION:::text:modified\n"
		"rootCap:DOMCharacterDataModified:CAPTURING_PHASE:#text:root:B:-:MODIFICATION:::text:modified\n"
		"textCap:DOMCharacterDataModified:AT_TARGET:#text:#text:B:-:MODIFICATION:::text:modified\n"
		"text:DOMCharacterDataModified:AT_TARGET:#text:#text:B:-:MODIFICATION:::text:modified\n"
		"root:DOMCharacterDataModified:BUBBLING_PHASE:#text:root:B:-:MODIFICATION:::text:modified\n"
		"doc:DOMCharacterDataModified:BUBBLING_PHASE:#text:#document:B:-:MODIFICATION:::text:modified\n"
	);
}


void EventTest::testCancel()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Element> pRoot = pDoc->createElement("root");
	AutoPtr<Text> pText = pDoc->createTextNode("text");
	pRoot->appendChild(pText);
	pDoc->appendChild(pRoot);

	TestEventListener docListener("doc");
	TestEventListener docCapListener("docCap", true);
	TestEventListener rootListener("root");
	TestEventListener rootCapListener("rootCap");
	TestEventListener textListener("text");
	TestEventListener textCapListener("textCap");

	pDoc->addEventListener(MutationEvent::DOMCharacterDataModified, &docListener, false);
	pDoc->addEventListener(MutationEvent::DOMCharacterDataModified, &docCapListener, true);	
	pRoot->addEventListener(MutationEvent::DOMCharacterDataModified, &rootListener, false);
	pRoot->addEventListener(MutationEvent::DOMCharacterDataModified, &rootCapListener, true);
	pText->addEventListener(MutationEvent::DOMCharacterDataModified, &textListener, false);
	pText->addEventListener(MutationEvent::DOMCharacterDataModified, &textCapListener, true);

	TestEventListener::reset();
	
	pText->setData("modified");

	const XMLString& log = TestEventListener::log();
	assert (log == "docCap:DOMCharacterDataModified:CAPTURING_PHASE:#text:#document:B:-:MODIFICATION:::text:modified\n");
}


void EventTest::testAttributes()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Element> pRoot = pDoc->createElement("root");

	TestEventListener rootListener("root");
	pRoot->addEventListener(MutationEvent::DOMAttrModified, &rootListener, false);
	
	pRoot->setAttribute("a1", "v1");

	const XMLString& log = TestEventListener::log();		
	assert (log == "root:DOMAttrModified:AT_TARGET:root:root:B:-:ADDITION:a1:a1::v1\n");
	
	TestEventListener::reset();
	pRoot->setAttribute("a1", "V1");
	assert (log == "root:DOMAttrModified:AT_TARGET:root:root:B:-:MODIFICATION:a1:a1:v1:V1\n");
	
	TestEventListener::reset();
	pRoot->setAttribute("a2", "v2");
	assert (log == "root:DOMAttrModified:AT_TARGET:root:root:B:-:ADDITION:a2:a2::v2\n");

	TestEventListener::reset();
	pRoot->removeAttribute("a1");	
	assert (log == "root:DOMAttrModified:AT_TARGET:root:root:B:-:REMOVAL:a1:a1:V1:\n");
}


void EventTest::testAddRemoveInEvent()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Element> pRoot = pDoc->createElement("root");

	TestEventListener docListener("doc", false, true, false);
	TestEventListener docCapListener("docCap", false, true, true);
	TestEventListener rootListener("root", false, true, false);
	TestEventListener rootCapListener("rootCap", false, true, true);

	pDoc->addEventListener(MutationEvent::DOMSubtreeModified, &docListener, false);
	pDoc->addEventListener(MutationEvent::DOMNodeInserted, &docListener, false);
	pDoc->addEventListener(MutationEvent::DOMNodeInsertedIntoDocument, &docListener, false);

	pDoc->addEventListener(MutationEvent::DOMSubtreeModified, &docCapListener, true);
	pDoc->addEventListener(MutationEvent::DOMNodeInserted, &docCapListener, true);
	pDoc->addEventListener(MutationEvent::DOMNodeInsertedIntoDocument, &docCapListener, true);
	
	pRoot->addEventListener(MutationEvent::DOMSubtreeModified, &rootListener, false);
	pRoot->addEventListener(MutationEvent::DOMNodeInserted, &rootListener, false);
	pRoot->addEventListener(MutationEvent::DOMNodeInsertedIntoDocument, &rootListener, false);

	pRoot->addEventListener(MutationEvent::DOMSubtreeModified, &rootCapListener, true);
	pRoot->addEventListener(MutationEvent::DOMNodeInserted, &rootCapListener, true);
	pRoot->addEventListener(MutationEvent::DOMNodeInsertedIntoDocument, &rootCapListener, true);
	
	pDoc->appendChild(pRoot);
	
	const XMLString& log = TestEventListener::log();
	assert (log == 
		"docCap:DOMNodeInserted:CAPTURING_PHASE:root:#document:B:-:MODIFICATION:#document:::\n"
		"rootCap:DOMNodeInserted:AT_TARGET:root:root:B:-:MODIFICATION:#document:::\n"
		"root:DOMNodeInserted:AT_TARGET:root:root:B:-:MODIFICATION:#document:::\n"
		"doc:DOMNodeInserted:BUBBLING_PHASE:root:#document:B:-:MODIFICATION:#document:::\n"
		"docCap:DOMNodeInsertedIntoDocument:CAPTURING_PHASE:root:#document:-:-:MODIFICATION::::\n"
		"rootCap:DOMNodeInsertedIntoDocument:AT_TARGET:root:root:-:-:MODIFICATION::::\n"
		"root:DOMNodeInsertedIntoDocument:AT_TARGET:root:root:-:-:MODIFICATION::::\n"
		"docCap:DOMSubtreeModified:AT_TARGET:#document:#document:B:-:MODIFICATION::::\n"
		"doc:DOMSubtreeModified:AT_TARGET:#document:#document:B:-:MODIFICATION::::\n"
	);
	
	TestEventListener::reset();
	
	AutoPtr<Text> pText = pDoc->createTextNode("text");
	pRoot->appendChild(pText);

	assert (log == 
		"docCap:DOMNodeInserted:CAPTURING_PHASE:#text:#document:B:-:MODIFICATION:root:::\n"
		"rootCap:DOMNodeInserted:CAPTURING_PHASE:#text:root:B:-:MODIFICATION:root:::\n"
		"root:DOMNodeInserted:BUBBLING_PHASE:#text:root:B:-:MODIFICATION:root:::\n"
		"doc:DOMNodeInserted:BUBBLING_PHASE:#text:#document:B:-:MODIFICATION:root:::\n"
		"docCap:DOMNodeInsertedIntoDocument:CAPTURING_PHASE:#text:#document:-:-:MODIFICATION::::\n"
		"rootCap:DOMNodeInsertedIntoDocument:CAPTURING_PHASE:#text:root:-:-:MODIFICATION::::\n"
		"docCap:DOMSubtreeModified:CAPTURING_PHASE:root:#document:B:-:MODIFICATION::::\n"
		"rootCap:DOMSubtreeModified:AT_TARGET:root:root:B:-:MODIFICATION::::\n"
		"root:DOMSubtreeModified:AT_TARGET:root:root:B:-:MODIFICATION::::\n"
		"doc:DOMSubtreeModified:BUBBLING_PHASE:root:#document:B:-:MODIFICATION::::\n"
	);
}


void EventTest::testSuspended()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Element> pRoot = pDoc->createElement("root");

	pDoc->suspendEvents();
	
	TestEventListener rootListener("root");
	pRoot->addEventListener(MutationEvent::DOMAttrModified, &rootListener, false);
	
	pRoot->setAttribute("a1", "v1");

	const XMLString& log = TestEventListener::log();		
	assert (log.empty());
	
	TestEventListener::reset();
	pRoot->setAttribute("a1", "V1");
	assert (log.empty());
	
	TestEventListener::reset();
	pRoot->setAttribute("a2", "v2");
	assert (log.empty());

	TestEventListener::reset();
	pRoot->removeAttribute("a1");	
	assert (log.empty());
}


void EventTest::setUp()
{
	TestEventListener::reset();
}


void EventTest::tearDown()
{
}


CppUnit::Test* EventTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("EventTest");

	CppUnit_addTest(pSuite, EventTest, testInsert);
	CppUnit_addTest(pSuite, EventTest, testInsertSubtree);
	CppUnit_addTest(pSuite, EventTest, testRemove);
	CppUnit_addTest(pSuite, EventTest, testRemoveSubtree);
	CppUnit_addTest(pSuite, EventTest, testCharacterData);
	CppUnit_addTest(pSuite, EventTest, testCancel);
	CppUnit_addTest(pSuite, EventTest, testAttributes);
	CppUnit_addTest(pSuite, EventTest, testAddRemoveInEvent);
	CppUnit_addTest(pSuite, EventTest, testSuspended);

	return pSuite;
}
