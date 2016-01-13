//
// StreamConverterTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/StreamConverterTest.h#1 $
//
// Definition of the StreamConverterTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef StreamConverterTest_INCLUDED
#define StreamConverterTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class StreamConverterTest: public CppUnit::TestCase
{
public:
	StreamConverterTest(const std::string& name);
	~StreamConverterTest();

	void testIdentityASCIIIn();
	void testIdentityASCIIOut();
	void testIdentityUTF8In();
	void testIdentityUTF8Out();
	void testUTF8toASCIIIn();
	void testUTF8toASCIIOut();
	void testLatin1toUTF8In();
	void testLatin1toUTF8Out();
	void testErrorsIn();
	void testErrorsOut();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // StreamConverterTest_INCLUDED
