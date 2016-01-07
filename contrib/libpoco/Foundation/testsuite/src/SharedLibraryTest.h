//
// SharedLibraryTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/SharedLibraryTest.h#1 $
//
// Definition of the SharedLibraryTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef SharedLibraryTest_INCLUDED
#define SharedLibraryTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class SharedLibraryTest: public CppUnit::TestCase
{
public:
	SharedLibraryTest(const std::string& name);
	~SharedLibraryTest();

	void testSharedLibrary1();
	void testSharedLibrary2();
	void testSharedLibrary3();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // SharedLibraryTest_INCLUDED
