//
// SimpleFileChannelTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/SimpleFileChannelTest.h#1 $
//
// Definition of the SimpleFileChannelTest class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef SimpleFileChannelTest_INCLUDED
#define SimpleFileChannelTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class SimpleFileChannelTest: public CppUnit::TestCase
{
public:
	SimpleFileChannelTest(const std::string& name);
	~SimpleFileChannelTest();

	void testRotate();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
	void remove(const std::string& baseName);
	std::string filename() const;
};


#endif // SimpleFileChannelTest_INCLUDED
