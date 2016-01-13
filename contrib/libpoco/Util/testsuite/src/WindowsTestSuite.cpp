//
// WindowsTestSuite.cpp
//
// $Id: //poco/1.4/Util/testsuite/src/WindowsTestSuite.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "WindowsTestSuite.h"
#include "WinRegistryTest.h"
#include "WinConfigurationTest.h"


CppUnit::Test* WindowsTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("WindowsTestSuite");

	pSuite->addTest(WinRegistryTest::suite());
	pSuite->addTest(WinConfigurationTest::suite());

	return pSuite;
}
