//
// ECTest.h
//
//
// Definition of the ECTest class.
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef ECTest_INCLUDED
#define ECTest_INCLUDED


#include "Poco/Crypto/Crypto.h"
#include "CppUnit/TestCase.h"


class ECTest: public CppUnit::TestCase
{
public:
	ECTest(const std::string& name);
	~ECTest();

	void testECNewKeys();
	void testECNewKeysNoPassphrase();
	void testECDSASignSha256();
	void testECDSASignManipulated();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // ECTest_INCLUDED
