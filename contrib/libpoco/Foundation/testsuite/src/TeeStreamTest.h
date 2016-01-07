//
// TeeStreamTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/TeeStreamTest.h#1 $
//
// Definition of the TeeStreamTest class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef TeeStreamTest_INCLUDED
#define TeeStreamTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class TeeStreamTest: public CppUnit::TestCase
{
public:
	TeeStreamTest(const std::string& name);
	~TeeStreamTest();

	void testTeeInputStream();
	void testTeeOutputStream();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // TeeStreamTest_INCLUDED
