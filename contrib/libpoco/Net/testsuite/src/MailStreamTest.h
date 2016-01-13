//
// MailStreamTest.h
//
// $Id: //poco/1.4/Net/testsuite/src/MailStreamTest.h#1 $
//
// Definition of the MailStreamTest class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MailStreamTest_INCLUDED
#define MailStreamTest_INCLUDED


#include "Poco/Net/Net.h"
#include "CppUnit/TestCase.h"


class MailStreamTest: public CppUnit::TestCase
{
public:
	MailStreamTest(const std::string& name);
	~MailStreamTest();

	void testMailInputStream();
	void testMailOutputStream();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // MailStreamTest_INCLUDED
