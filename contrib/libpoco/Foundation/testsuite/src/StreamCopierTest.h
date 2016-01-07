//
// StreamCopierTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/StreamCopierTest.h#2 $
//
// Definition of the StreamCopierTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef StreamCopierTest_INCLUDED
#define StreamCopierTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class StreamCopierTest: public CppUnit::TestCase
{
public:
	StreamCopierTest(const std::string& name);
	~StreamCopierTest();

	void testBufferedCopy();
	void testUnbufferedCopy();
	void testCopyToString();
#if defined(POCO_HAVE_INT64)
	void testBufferedCopy64();
	void testUnbufferedCopy64();
	void testCopyToString64();
#endif

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // StreamCopierTest_INCLUDED
