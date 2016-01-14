//
// ZipTestSuite.cpp
//
// $Id: //poco/1.4/Zip/testsuite/src/ZipTestSuite.cpp#1 $
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "ZipTestSuite.h"
#include "ZipTest.h"
#include "PartialStreamTest.h"
#include "CompressTest.h"


CppUnit::Test* ZipTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("ZipTestSuite");

	pSuite->addTest(ZipTest::suite());
	pSuite->addTest(PartialStreamTest::suite());
	pSuite->addTest(CompressTest::suite());

	return pSuite;
}
