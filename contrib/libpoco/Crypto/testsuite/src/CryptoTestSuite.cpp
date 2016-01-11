//
// CryptoTestSuite.cpp
//
// $Id: //poco/1.4/Crypto/testsuite/src/CryptoTestSuite.cpp#1 $
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CryptoTestSuite.h"
#include "CryptoTest.h"
#include "RSATest.h"
#include "DigestEngineTest.h"


CppUnit::Test* CryptoTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("CryptoTestSuite");

	pSuite->addTest(CryptoTest::suite());
	pSuite->addTest(RSATest::suite());
	pSuite->addTest(DigestEngineTest::suite());

	return pSuite;
}
