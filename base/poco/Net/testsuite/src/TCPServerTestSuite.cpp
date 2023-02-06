//
// TCPServerTestSuite.cpp
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "TCPServerTestSuite.h"
#include "TCPServerTest.h"


CppUnit::Test* TCPServerTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("TCPServerTestSuite");

	pSuite->addTest(TCPServerTest::suite());

	return pSuite;
}
