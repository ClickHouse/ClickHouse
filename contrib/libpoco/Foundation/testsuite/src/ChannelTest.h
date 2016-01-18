//
// ChannelTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/ChannelTest.h#1 $
//
// Definition of the ChannelTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef ChannelTest_INCLUDED
#define ChannelTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class ChannelTest: public CppUnit::TestCase
{
public:
	ChannelTest(const std::string& name);
	~ChannelTest();

	void testSplitter();
	void testAsync();
	void testFormatting();
	void testConsole();
	void testStream();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // ChannelTest_INCLUDED
