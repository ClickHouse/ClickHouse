//
// DirectoryIteratorsTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/DirectoryIteratorsTest.h#1 $
//
// Definition of the DirectoryIteratorsTest class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DirectoryIteratorsTest_INCLUDED
#define DirectoryIteratorsTest_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Path.h"
#include "CppUnit/TestCase.h"


class DirectoryIteratorsTest: public CppUnit::TestCase
{
public:
	DirectoryIteratorsTest(const std::string& name);
	~DirectoryIteratorsTest();

	void testDirectoryIterator();
	void testSortedDirectoryIterator();
	void testSimpleRecursiveDirectoryIterator();
	void testSiblingsFirstRecursiveDirectoryIterator();
	
	void setUp();
	void tearDown();

	static CppUnit::Test* suite();
	
protected:
	Poco::Path path() const;
	void createSubdir(Poco::Path& p);

private:
};


#endif // DirectoryIteratorsTest_INCLUDED
