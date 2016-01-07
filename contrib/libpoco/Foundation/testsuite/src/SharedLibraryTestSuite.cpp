//
// SharedLibraryTestSuite.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/SharedLibraryTestSuite.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "SharedLibraryTestSuite.h"
#include "ClassLoaderTest.h"
#include "ManifestTest.h"
#include "SharedLibraryTest.h"


CppUnit::Test* SharedLibraryTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("SharedLibraryTestSuite");

	pSuite->addTest(ManifestTest::suite());

#if !defined(_WIN32) || defined(_DLL)
	pSuite->addTest(SharedLibraryTest::suite());
	pSuite->addTest(ClassLoaderTest::suite());
#endif

	return pSuite;
}
