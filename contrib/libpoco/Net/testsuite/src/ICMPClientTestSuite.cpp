//
// ICMPClientTestSuite.cpp
//
// $Id: //poco/1.4/Net/testsuite/src/ICMPClientTestSuite.cpp#1 $
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "ICMPClientTestSuite.h"
#include "ICMPClientTest.h"


CppUnit::Test* ICMPClientTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("ICMPClientTestSuite");

	pSuite->addTest(ICMPClientTest::suite());

	return pSuite;
}
