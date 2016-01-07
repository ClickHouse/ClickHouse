//
// MultipartWriterTest.h
//
// $Id: //poco/1.4/Net/testsuite/src/MultipartWriterTest.h#1 $
//
// Definition of the MultipartWriterTest class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MultipartWriterTest_INCLUDED
#define MultipartWriterTest_INCLUDED


#include "Poco/Net/Net.h"
#include "CppUnit/TestCase.h"


class MultipartWriterTest: public CppUnit::TestCase
{
public:
	MultipartWriterTest(const std::string& name);
	~MultipartWriterTest();

	void testWriteOnePart();
	void testWriteTwoParts();
	void testBoundary();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // MultipartWriterTest_INCLUDED
