//
// UUIDTestSuite.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/UUIDTestSuite.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "UUIDTestSuite.h"
#include "UUIDTest.h"
#include "UUIDGeneratorTest.h"


CppUnit::Test* UUIDTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("UUIDTestSuite");

	pSuite->addTest(UUIDTest::suite());
	pSuite->addTest(UUIDGeneratorTest::suite());

	return pSuite;
}
