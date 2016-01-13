//
// MailMessageTest.h
//
// $Id: //poco/1.4/Net/testsuite/src/MailMessageTest.h#1 $
//
// Definition of the MailMessageTest class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MailMessageTest_INCLUDED
#define MailMessageTest_INCLUDED


#include "Poco/Net/Net.h"
#include "CppUnit/TestCase.h"


class MailMessageTest: public CppUnit::TestCase
{
public:
	MailMessageTest(const std::string& name);
	~MailMessageTest();

	void testWriteQP();
	void testWrite8Bit();
	void testWriteBase64();
	void testWriteManyRecipients();
	void testWriteMultiPart();
	void testReadWriteMultiPart();
	void testReadWriteMultiPartStore();
	void testReadDefaultTransferEncoding();
	void testReadQP();
	void testRead8Bit();
	void testReadMultiPart();
	void testReadMultiPartDefaultTransferEncoding();
	void testEncodeWord();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // MailMessageTest_INCLUDED
