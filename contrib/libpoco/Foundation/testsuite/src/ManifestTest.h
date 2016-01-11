//
// ManifestTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/ManifestTest.h#1 $
//
// Definition of the ManifestTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef ManifestTest_INCLUDED
#define ManifestTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class ManifestTest: public CppUnit::TestCase
{
public:
	ManifestTest(const std::string& name);
	~ManifestTest();

	void testManifest();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // ManifestTest_INCLUDED
