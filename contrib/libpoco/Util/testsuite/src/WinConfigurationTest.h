//
// WinConfigurationTest.h
//
// $Id: //poco/1.4/Util/testsuite/src/WinConfigurationTest.h#1 $
//
// Definition of the WinConfigurationTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef WinConfigurationTest_INCLUDED
#define WinConfigurationTest_INCLUDED


#include "Poco/Util/Util.h"
#include "CppUnit/TestCase.h"


class WinConfigurationTest: public CppUnit::TestCase
{
public:
	WinConfigurationTest(const std::string& name);
	~WinConfigurationTest();

	void testConfiguration();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // WinConfigurationTest_INCLUDED
