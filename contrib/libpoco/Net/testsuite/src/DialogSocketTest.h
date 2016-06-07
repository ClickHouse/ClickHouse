//
// DialogSocketTest.h
//
// $Id: //poco/1.4/Net/testsuite/src/DialogSocketTest.h#1 $
//
// Definition of the DialogSocketTest class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DialogSocketTest_INCLUDED
#define DialogSocketTest_INCLUDED


#include "Poco/Net/Net.h"
#include "CppUnit/TestCase.h"


class DialogSocketTest: public CppUnit::TestCase
{
public:
	DialogSocketTest(const std::string& name);
	~DialogSocketTest();

	void testDialogSocket();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // DialogSocketTest_INCLUDED
