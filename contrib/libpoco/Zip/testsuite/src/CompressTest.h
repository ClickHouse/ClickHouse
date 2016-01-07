//
// CompressTest.h
//
// $Id: //poco/1.4/Zip/testsuite/src/CompressTest.h#1 $
//
// Definition of the CompressTest class.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CompressTest_INCLUDED
#define CompressTest_INCLUDED


#include "Poco/Zip/Zip.h"
#include "CppUnit/TestCase.h"


class CompressTest: public CppUnit::TestCase
{
public:
	CompressTest(const std::string& name);
	~CompressTest();

	void testSingleFile();
	void testDirectory();
	void testManipulator();
	void testManipulatorDel();
	void testManipulatorReplace();
	void testSetZipComment();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // CompressTest_INCLUDED
