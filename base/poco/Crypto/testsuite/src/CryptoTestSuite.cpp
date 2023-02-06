//
// CryptoTestSuite.cpp
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Platform.h"
// see https://www.openssl.org/docs/faq.html
// and https://github.com/openssl/openssl/blob/master/doc/man3/OPENSSL_Applink.pod
#if defined(POCO_OS_FAMILY_WINDOWS)
#include "openssl/applink.c"
#endif
#include "CryptoTestSuite.h"
#include "CryptoTest.h"
#include "RSATest.h"
#include "ECTest.h"
#include "EVPTest.h"
#include "DigestEngineTest.h"
#include "PKCS12ContainerTest.h"


CppUnit::Test* CryptoTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("CryptoTestSuite");

	pSuite->addTest(CryptoTest::suite());
	pSuite->addTest(RSATest::suite());
	pSuite->addTest(ECTest::suite());
	pSuite->addTest(EVPTest::suite());
	pSuite->addTest(DigestEngineTest::suite());
	pSuite->addTest(PKCS12ContainerTest::suite());
	return pSuite;
}
