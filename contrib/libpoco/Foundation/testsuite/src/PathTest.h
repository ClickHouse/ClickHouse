//
// PathTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/PathTest.h#2 $
//
// Definition of the PathTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef PathTest_INCLUDED
#define PathTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class PathTest: public CppUnit::TestCase
{
public:
	PathTest(const std::string& name);
	~PathTest();

	void testParseUnix1();
	void testParseUnix2();
	void testParseUnix3();
	void testParseUnix4();
	void testParseUnix5();
	void testParseWindows1();
	void testParseWindows2();
	void testParseWindows3();
	void testParseWindows4();
	void testParseVMS1();
	void testParseVMS2();
	void testParseVMS3();
	void testParseVMS4();
	void testParseGuess();
	void testTryParse();
	void testStatics();
	void testBaseNameExt();
	void testAbsolute();
	void testRobustness();
	void testParent();
	void testForDirectory();
	void testExpand();
	void testListRoots();
	void testFind();
	void testSwap();
	void testResolve();
	void testPushPop();
	void testWindowsSystem();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // PathTest_INCLUDED
