//
// HTMLFormTest.h
//
// $Id: //poco/1.4/Net/testsuite/src/HTMLFormTest.h#2 $
//
// Definition of the HTMLFormTest class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef HTMLFormTest_INCLUDED
#define HTMLFormTest_INCLUDED


#include "Poco/Net/Net.h"
#include "CppUnit/TestCase.h"


class HTMLFormTest: public CppUnit::TestCase
{
public:
	HTMLFormTest(const std::string& name);
	~HTMLFormTest();

	void testWriteUrl();
	void testWriteMultipart();
	void testReadUrlGET();
	void testReadUrlPOST();
	void testReadUrlPUT();
	void testReadUrlBOM();
	void testReadMultipart();
	void testSubmit1();
	void testSubmit2();
	void testSubmit3();
	void testFieldLimitUrl();
	void testFieldLimitMultipart();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // HTMLFormTest_INCLUDED
