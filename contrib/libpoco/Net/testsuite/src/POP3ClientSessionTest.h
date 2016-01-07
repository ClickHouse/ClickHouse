//
// POP3ClientSessionTest.h
//
// $Id: //poco/1.4/Net/testsuite/src/POP3ClientSessionTest.h#1 $
//
// Definition of the POP3ClientSessionTest class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef POP3ClientSessionTest_INCLUDED
#define POP3ClientSessionTest_INCLUDED


#include "Poco/Net/Net.h"
#include "CppUnit/TestCase.h"


class POP3ClientSessionTest: public CppUnit::TestCase
{
public:
	POP3ClientSessionTest(const std::string& name);
	~POP3ClientSessionTest();

	void testLogin();
	void testLoginFail();
	void testMessageCount();
	void testList();
	void testRetrieveMessage();
	void testRetrieveHeader();
	void testRetrieveMessages();
	void testDeleteMessage();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // POP3ClientSessionTest_INCLUDED
