//
// FileTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/FileTest.h#1 $
//
// Definition of the FileTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef FileTest_INCLUDED
#define FileTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class FileTest: public CppUnit::TestCase
{
public:
	FileTest(const std::string& name);
	~FileTest();

	void testFileAttributes1();
	void testCreateFile();
	void testFileAttributes2();
	void testFileAttributes3();
	void testCompare();
	void testSwap();
	void testSize();
	void testDirectory();
	void testCopy();
	void testMove();
	void testCopyDirectory();
	void testRename();
	void testRootDir();
	
	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // FileTest_INCLUDED
