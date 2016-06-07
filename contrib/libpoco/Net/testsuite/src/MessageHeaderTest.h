//
// MessageHeaderTest.h
//
// $Id: //poco/1.4/Net/testsuite/src/MessageHeaderTest.h#2 $
//
// Definition of the MessageHeaderTest class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MessageHeaderTest_INCLUDED
#define MessageHeaderTest_INCLUDED


#include "Poco/Net/Net.h"
#include "CppUnit/TestCase.h"


class MessageHeaderTest: public CppUnit::TestCase
{
public:
	MessageHeaderTest(const std::string& name);
	~MessageHeaderTest();

	void testWrite();
	void testRead1();
	void testRead2();
	void testRead3();
	void testRead4();
	void testRead5();
	void testReadFolding1();
	void testReadFolding2();
	void testReadFolding3();
	void testReadFolding4();
	void testReadFolding5();
	void testReadInvalid1();
	void testReadInvalid2();
	void testSplitElements();
	void testSplitParameters();
	void testFieldLimit();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // MessageHeaderTest_INCLUDED
