//
// EventTest.h
//
// $Id: //poco/1.4/XML/testsuite/src/EventTest.h#1 $
//
// Definition of the EventTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef EventTest_INCLUDED
#define EventTest_INCLUDED


#include "Poco/XML/XML.h"
#include "CppUnit/TestCase.h"


class EventTest: public CppUnit::TestCase
{
public:
	EventTest(const std::string& name);
	~EventTest();

	void testInsert();
	void testInsertSubtree();
	void testRemove();
	void testRemoveSubtree();
	void testCharacterData();
	void testCancel();
	void testAttributes();
	void testAddRemoveInEvent();
	void testSuspended();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // EventTest_INCLUDED
