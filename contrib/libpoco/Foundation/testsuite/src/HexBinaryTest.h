//
// HexBinaryTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/HexBinaryTest.h#1 $
//
// Definition of the HexBinaryTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef HexBinaryTest_INCLUDED
#define HexBinaryTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class HexBinaryTest: public CppUnit::TestCase
{
public:
	HexBinaryTest(const std::string& name);
	~HexBinaryTest();

	void testEncoder();
	void testDecoder();
	void testEncodeDecode();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // HexBinaryTest_INCLUDED
