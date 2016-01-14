//
// ActiveDispatcherTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/ActiveDispatcherTest.h#1 $
//
// Definition of the ActiveDispatcherTest class.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef ActiveDispatcherTest_INCLUDED
#define ActiveDispatcherTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class ActiveDispatcherTest: public CppUnit::TestCase
{
public:
	ActiveDispatcherTest(const std::string& name);
	~ActiveDispatcherTest();

	void testWait();
	void testWaitInterval();
	void testTryWait();
	void testFailure();
	void testVoid();
	void testVoidIn();
	void testVoidInOut();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // ActiveDispatcherTest_INCLUDED
