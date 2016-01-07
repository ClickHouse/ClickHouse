//
// HTTPServerTestSuite.cpp
//
// $Id: //poco/1.4/Net/testsuite/src/HTTPServerTestSuite.cpp#1 $
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "HTTPServerTestSuite.h"
#include "HTTPServerTest.h"


CppUnit::Test* HTTPServerTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("HTTPServerTestSuite");

	pSuite->addTest(HTTPServerTest::suite());

	return pSuite;
}
