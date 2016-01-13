//
// GlobTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/GlobTest.h#1 $
//
// Definition of the GlobTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef GlobTest_INCLUDED
#define GlobTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"
#include <set>


class GlobTest: public CppUnit::TestCase
{
public:
	GlobTest(const std::string& name);
	~GlobTest();

	void testMatchChars();
	void testMatchQM();
	void testMatchAsterisk();
	void testMatchRange();
	void testMisc();
	void testGlob();
	void testCaseless();
	void testMatchEmptyPattern();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
	void createFile(const std::string& path);
	void translatePaths(std::set<std::string>& paths);
};


#endif // GlobTest_INCLUDED
