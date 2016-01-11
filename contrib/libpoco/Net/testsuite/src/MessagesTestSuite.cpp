//
// MessagesTestSuite.cpp
//
// $Id: //poco/1.4/Net/testsuite/src/MessagesTestSuite.cpp#1 $
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "MessagesTestSuite.h"
#include "NameValueCollectionTest.h"
#include "MessageHeaderTest.h"
#include "MediaTypeTest.h"
#include "MultipartWriterTest.h"
#include "MultipartReaderTest.h"
#include "QuotedPrintableTest.h"


CppUnit::Test* MessagesTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("MessagesTestSuite");

	pSuite->addTest(NameValueCollectionTest::suite());
	pSuite->addTest(MessageHeaderTest::suite());
	pSuite->addTest(MediaTypeTest::suite());
	pSuite->addTest(MultipartWriterTest::suite());
	pSuite->addTest(MultipartReaderTest::suite());
	pSuite->addTest(QuotedPrintableTest::suite());

	return pSuite;
}
