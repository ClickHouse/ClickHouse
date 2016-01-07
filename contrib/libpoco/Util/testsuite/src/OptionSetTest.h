//
// OptionSetTest.h
//
// $Id: //poco/1.4/Util/testsuite/src/OptionSetTest.h#1 $
//
// Definition of the OptionSetTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef OptionSetTest_INCLUDED
#define OptionSetTest_INCLUDED


#include "Poco/Util/Util.h"
#include "CppUnit/TestCase.h"


class OptionSetTest: public CppUnit::TestCase
{
public:
	OptionSetTest(const std::string& name);
	~OptionSetTest();

	void testOptionSet();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // OptionSetTest_INCLUDED
