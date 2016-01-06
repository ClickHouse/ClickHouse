//
// OAuthTestSuite.cpp
//
// $Id$
//
// Copyright (c) 2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "OAuthTestSuite.h"
#include "OAuth10CredentialsTest.h"
#include "OAuth20CredentialsTest.h"


CppUnit::Test* OAuthTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("OAuthTestSuite");

	pSuite->addTest(OAuth10CredentialsTest::suite());
	pSuite->addTest(OAuth20CredentialsTest::suite());

	return pSuite;
}
