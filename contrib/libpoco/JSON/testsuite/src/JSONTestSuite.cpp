//
// JSONTestSuite.cpp
//
// $Id: //poco/1.4/JSON/testsuite/src/JSONTestSuite.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "JSONTestSuite.h"
#include "JSONTest.h"


CppUnit::Test* JSONTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("JSONTestSuite");

	pSuite->addTest(JSONTest::suite());

	return pSuite;
}
