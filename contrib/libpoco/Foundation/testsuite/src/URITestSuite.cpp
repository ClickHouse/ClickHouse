//
// URITestSuite.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/URITestSuite.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "URITestSuite.h"
#include "URITest.h"
#include "URIStreamOpenerTest.h"


CppUnit::Test* URITestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("URITestSuite");

	pSuite->addTest(URITest::suite());
	pSuite->addTest(URIStreamOpenerTest::suite());

	return pSuite;
}
