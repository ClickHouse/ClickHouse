//
// FTPClientTestSuite.cpp
//
// $Id: //poco/svn/Net/testsuite/src/FTPClientTestSuite.cpp#2 $
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "FTPClientTestSuite.h"
#include "FTPClientSessionTest.h"
#include "FTPStreamFactoryTest.h"


CppUnit::Test* FTPClientTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("FTPClientTestSuite");

	pSuite->addTest(FTPClientSessionTest::suite());
	pSuite->addTest(FTPStreamFactoryTest::suite());

	return pSuite;
}
