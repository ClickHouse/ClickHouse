//
// HelpFormatterTest.h
//
// $Id: //poco/1.4/Util/testsuite/src/HelpFormatterTest.h#1 $
//
// Definition of the HelpFormatterTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef HelpFormatterTest_INCLUDED
#define HelpFormatterTest_INCLUDED


#include "Poco/Util/Util.h"
#include "CppUnit/TestCase.h"


class HelpFormatterTest: public CppUnit::TestCase
{
public:
	HelpFormatterTest(const std::string& name);
	~HelpFormatterTest();

	void testHelpFormatter();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // HelpFormatterTest_INCLUDED
