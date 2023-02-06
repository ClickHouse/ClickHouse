//
// ReactorTestSuite.cpp
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "ReactorTestSuite.h"
#include "SocketReactorTest.h"


CppUnit::Test* ReactorTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("ReactorTestSuite");

	pSuite->addTest(SocketReactorTest::suite());

	return pSuite;
}
