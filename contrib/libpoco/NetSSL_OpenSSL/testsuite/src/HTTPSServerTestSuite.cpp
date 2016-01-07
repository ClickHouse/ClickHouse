//
// HTTPSServerTestSuite.cpp
//
// $Id: //poco/1.4/NetSSL_OpenSSL/testsuite/src/HTTPSServerTestSuite.cpp#1 $
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "HTTPSServerTestSuite.h"
#include "HTTPSServerTest.h"


CppUnit::Test* HTTPSServerTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("HTTPSServerTestSuite");

	pSuite->addTest(HTTPSServerTest::suite());

	return pSuite;
}
