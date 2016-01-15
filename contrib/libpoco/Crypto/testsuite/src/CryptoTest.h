//
// CryptoTest.h
//
// $Id: //poco/1.4/Crypto/testsuite/src/CryptoTest.h#2 $
//
// Definition of the CryptoTest class.
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CryptoTest_INCLUDED
#define CryptoTest_INCLUDED


#include "Poco/Crypto/Crypto.h"
#include "CppUnit/TestCase.h"


class CryptoTest: public CppUnit::TestCase
{
public:
	enum 
	{
		MAX_DATA_SIZE = 10000
	};
	
	CryptoTest(const std::string& name);
	~CryptoTest();

	void testEncryptDecrypt();
	void testEncryptDecryptWithSalt();
	void testEncryptDecryptDESECB();
	void testStreams();
	void testPassword();
	void testEncryptInterop();
	void testDecryptInterop();
	void testCertificate();
	
	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // CryptoTest_INCLUDED
