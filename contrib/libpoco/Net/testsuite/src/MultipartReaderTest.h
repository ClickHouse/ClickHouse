//
// MultipartReaderTest.h
//
// $Id: //poco/1.4/Net/testsuite/src/MultipartReaderTest.h#1 $
//
// Definition of the MultipartReaderTest class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MultipartReaderTest_INCLUDED
#define MultipartReaderTest_INCLUDED


#include "Poco/Net/Net.h"
#include "CppUnit/TestCase.h"


class MultipartReaderTest: public CppUnit::TestCase
{
public:
	MultipartReaderTest(const std::string& name);
	~MultipartReaderTest();

	void testReadOnePart();
	void testReadTwoParts();
	void testReadEmptyLines();
	void testReadLongPart();
	void testGuessBoundary();
	void testPreamble();
	void testBadBoundary();
	void testRobustness();
	void testUnixLineEnds();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // MultipartReaderTest_INCLUDED
