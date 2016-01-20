//
// HTTPSClientTestSuite.cpp
//
// $Id: //poco/1.4/NetSSL_OpenSSL/testsuite/src/HTTPSClientTestSuite.cpp#1 $
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "HTTPSClientTestSuite.h"
#include "HTTPSClientSessionTest.h"
#include "HTTPSStreamFactoryTest.h"


CppUnit::Test* HTTPSClientTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("HTTPSClientTestSuite");

	pSuite->addTest(HTTPSClientSessionTest::suite());
	pSuite->addTest(HTTPSStreamFactoryTest::suite());

	return pSuite;
}
