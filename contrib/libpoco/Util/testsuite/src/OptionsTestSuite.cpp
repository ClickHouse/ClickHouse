//
// OptionsTestSuite.cpp
//
// $Id: //poco/1.4/Util/testsuite/src/OptionsTestSuite.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "OptionsTestSuite.h"
#include "OptionTest.h"
#include "OptionSetTest.h"
#include "HelpFormatterTest.h"
#include "OptionProcessorTest.h"
#include "ValidatorTest.h"


CppUnit::Test* OptionsTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("OptionsTestSuite");

	pSuite->addTest(OptionTest::suite());
	pSuite->addTest(OptionSetTest::suite());
	pSuite->addTest(HelpFormatterTest::suite());
	pSuite->addTest(OptionProcessorTest::suite());
	pSuite->addTest(ValidatorTest::suite());

	return pSuite;
}
