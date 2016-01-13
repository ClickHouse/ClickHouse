//
// ICMPSocketTest.h
//
// $Id: //poco/1.4/Net/testsuite/src/ICMPSocketTest.h#1 $
//
// Definition of the ICMPSocketTest class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef ICMPSocketTest_INCLUDED
#define ICMPSocketTest_INCLUDED


#include "Poco/Net/Net.h"
#include "CppUnit/TestCase.h"


class ICMPSocketTest: public CppUnit::TestCase
{
public:
	ICMPSocketTest(const std::string& name);
	~ICMPSocketTest();

	void testSendToReceiveFrom();
	void testAssign();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // ICMPSocketTest_INCLUDED
