//
// CryptoTestSuite.h
//
// $Id: //poco/1.4/Crypto/testsuite/src/CryptoTestSuite.h#1 $
//
// Definition of the CryptoTestSuite class.
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CryptoTestSuite_INCLUDED
#define CryptoTestSuite_INCLUDED


#include "CppUnit/TestSuite.h"


class CryptoTestSuite
{
public:
	static CppUnit::Test* suite();
};


#endif // CryptoTestSuite_INCLUDED
