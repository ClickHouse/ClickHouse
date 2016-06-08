//
// HTMLTestSuite.cpp
//
// $Id: //poco/1.4/Net/testsuite/src/HTMLTestSuite.cpp#1 $
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "HTMLTestSuite.h"
#include "HTMLFormTest.h"


CppUnit::Test* HTMLTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("HTMLTestSuite");

	pSuite->addTest(HTMLFormTest::suite());

	return pSuite;
}
