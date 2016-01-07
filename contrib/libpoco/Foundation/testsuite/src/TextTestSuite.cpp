//
// TextTestSuite.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/TextTestSuite.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "TextTestSuite.h"
#include "TextIteratorTest.h"
#include "TextBufferIteratorTest.h"
#include "TextConverterTest.h"
#include "StreamConverterTest.h"
#include "TextEncodingTest.h"
#include "UTF8StringTest.h"
#ifndef POCO_NO_WSTRING
#include "UnicodeConverterTest.h"
#endif

CppUnit::Test* TextTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("TextTestSuite");

	pSuite->addTest(TextIteratorTest::suite());
	pSuite->addTest(TextBufferIteratorTest::suite());
	pSuite->addTest(TextConverterTest::suite());
	pSuite->addTest(StreamConverterTest::suite());
	pSuite->addTest(TextEncodingTest::suite());
	pSuite->addTest(UTF8StringTest::suite());
#ifndef POCO_NO_WSTRING
	pSuite->addTest(UnicodeConverterTest::suite());
#endif

	return pSuite;
}
