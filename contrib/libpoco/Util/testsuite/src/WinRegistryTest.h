//
// WinRegistryTest.h
//
// $Id: //poco/1.4/Util/testsuite/src/WinRegistryTest.h#1 $
//
// Definition of the WinRegistryTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef WinRegistryTest_INCLUDED
#define WinRegistryTest_INCLUDED


#include "Poco/Util/Util.h"
#include "CppUnit/TestCase.h"


class WinRegistryTest: public CppUnit::TestCase
{
public:
	WinRegistryTest(const std::string& name);
	~WinRegistryTest();

	void testRegistry();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // WinRegistryTest_INCLUDED
