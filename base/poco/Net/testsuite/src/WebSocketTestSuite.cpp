//
// WebSocketTestSuite.cpp
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "WebSocketTestSuite.h"
#include "WebSocketTest.h"


CppUnit::Test* WebSocketTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("WebSocketTestSuite");

	pSuite->addTest(WebSocketTest::suite());

	return pSuite;
}
