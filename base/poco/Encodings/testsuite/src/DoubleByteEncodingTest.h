//
// DoubleByteEncodingTest.h
//
// Definition of the DoubleByteEncodingTest class.
//
// Copyright (c) 2018, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier: Apache-2.0
//


#ifndef DoubleByteEncodingTest_INCLUDED
#define DoubleByteEncodingTest_INCLUDED


#include "CppUnit/TestCase.h"


class DoubleByteEncodingTest: public CppUnit::TestCase
{
public:
	DoubleByteEncodingTest(const std::string& name);
	~DoubleByteEncodingTest();

	void testSingleByte();
	void testSingleByteReverse();
	void testDoubleByte();
	void testDoubleByteReverse();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // DoubleByteEncodingTest_INCLUDED
