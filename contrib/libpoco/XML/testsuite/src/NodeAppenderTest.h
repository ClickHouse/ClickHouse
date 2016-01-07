//
// NodeAppenderTest.h
//
// $Id: //poco/1.4/XML/testsuite/src/NodeAppenderTest.h#1 $
//
// Definition of the NodeAppenderTest class.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NodeAppenderTest_INCLUDED
#define NodeAppenderTest_INCLUDED


#include "Poco/XML/XML.h"
#include "CppUnit/TestCase.h"


class NodeAppenderTest: public CppUnit::TestCase
{
public:
	NodeAppenderTest(const std::string& name);
	~NodeAppenderTest();

	void testAppendNode();
	void testAppendNodeList();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // NodeAppenderTest_INCLUDED
