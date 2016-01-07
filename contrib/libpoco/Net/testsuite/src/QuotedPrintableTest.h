//
// QuotedPrintableTest.h
//
// $Id: //poco/1.4/Net/testsuite/src/QuotedPrintableTest.h#1 $
//
// Definition of the QuotedPrintableTest class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef QuotedPrintableTest_INCLUDED
#define QuotedPrintableTest_INCLUDED


#include "Poco/Net/Net.h"
#include "CppUnit/TestCase.h"


class QuotedPrintableTest: public CppUnit::TestCase
{
public:
	QuotedPrintableTest(const std::string& name);
	~QuotedPrintableTest();

	void testEncode();
	void testDecode();
	
	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // QuotedPrintableTest_INCLUDED
