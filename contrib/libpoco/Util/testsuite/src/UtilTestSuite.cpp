//
// UtilTestSuite.cpp
//
// $Id: //poco/1.4/Util/testsuite/src/UtilTestSuite.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "UtilTestSuite.h"
#include "ConfigurationTestSuite.h"
#include "OptionsTestSuite.h"
#include "TimerTestSuite.h"
#if defined(_MSC_VER) && !defined(_WIN32_WCE)
#include "WindowsTestSuite.h"
#endif


CppUnit::Test* UtilTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("UtilTestSuite");

	pSuite->addTest(ConfigurationTestSuite::suite());
	pSuite->addTest(OptionsTestSuite::suite());
	pSuite->addTest(TimerTestSuite::suite());
#if defined(_MSC_VER) && !defined(_WIN32_WCE)
	pSuite->addTest(WindowsTestSuite::suite());
#endif

	return pSuite;
}
