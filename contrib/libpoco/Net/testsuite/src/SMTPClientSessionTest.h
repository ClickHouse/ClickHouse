//
// SMTPClientSessionTest.h
//
// $Id: //poco/1.4/Net/testsuite/src/SMTPClientSessionTest.h#1 $
//
// Definition of the SMTPClientSessionTest class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef SMTPClientSessionTest_INCLUDED
#define SMTPClientSessionTest_INCLUDED


#include "Poco/Net/Net.h"
#include "CppUnit/TestCase.h"


class SMTPClientSessionTest: public CppUnit::TestCase
{
public:
	SMTPClientSessionTest(const std::string& name);
	~SMTPClientSessionTest();

	void testLoginEHLO();
	void testLoginHELO();
	void testLoginFailed();
	void testSend();
	void testSendMultiRecipient();
	void testMultiSeparateRecipient();
	void testSendFailed();
	
	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // SMTPClientSessionTest_INCLUDED
