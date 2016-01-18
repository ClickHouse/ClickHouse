//
// RSATest.h
//
// $Id: //poco/1.4/Crypto/testsuite/src/RSATest.h#1 $
//
// Definition of the RSATest class.
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef RSATest_INCLUDED
#define RSATest_INCLUDED


#include "Poco/Crypto/Crypto.h"
#include "CppUnit/TestCase.h"


class RSATest: public CppUnit::TestCase
{
public:
	RSATest(const std::string& name);
	~RSATest();

	void testNewKeys();
	void testNewKeysNoPassphrase();
	void testSign();
	void testSignSha256();
	void testSignManipulated();
	void testRSACipher();
	void testRSACipherLarge();
	void testCertificate();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // RSATest_INCLUDED
