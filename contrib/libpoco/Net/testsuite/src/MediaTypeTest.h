//
// MediaTypeTest.h
//
// $Id: //poco/1.4/Net/testsuite/src/MediaTypeTest.h#2 $
//
// Definition of the MediaTypeTest class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MediaTypeTest_INCLUDED
#define MediaTypeTest_INCLUDED


#include "Poco/Net/Net.h"
#include "CppUnit/TestCase.h"


class MediaTypeTest: public CppUnit::TestCase
{
public:
	MediaTypeTest(const std::string& name);
	~MediaTypeTest();

	void testParse();
	void testToString();
	void testMatch();
	void testMatchRange();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // MediaTypeTest_INCLUDED
