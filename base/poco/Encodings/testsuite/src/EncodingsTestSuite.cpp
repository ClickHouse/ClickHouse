//
// EncodingsTestSuite.cpp
//
// Copyright (c) 2018, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier: Apache-2.0
//


#include "EncodingsTestSuite.h"
#include "DoubleByteEncodingTest.h"


CppUnit::Test* EncodingsTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("EncodingsTestSuite");

	pSuite->addTest(DoubleByteEncodingTest::suite());

	return pSuite;
}
