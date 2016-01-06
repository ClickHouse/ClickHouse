//
// HTTPTestSuite.cpp
//
// $Id: //poco/1.4/Net/testsuite/src/HTTPTestSuite.cpp#1 $
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "HTTPTestSuite.h"
#include "HTTPRequestTest.h"
#include "HTTPResponseTest.h"
#include "HTTPCookieTest.h"
#include "HTTPCredentialsTest.h"


CppUnit::Test* HTTPTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("HTTPTestSuite");

	pSuite->addTest(HTTPRequestTest::suite());
	pSuite->addTest(HTTPResponseTest::suite());
	pSuite->addTest(HTTPCookieTest::suite());
	pSuite->addTest(HTTPCredentialsTest::suite());

	return pSuite;
}
