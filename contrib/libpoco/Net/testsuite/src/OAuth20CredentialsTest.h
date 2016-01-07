//
// OAuth20CredentialsTest.h
//
// $Id$
//
// Definition of the OAuth20CredentialsTest class.
//
// Copyright (c) 2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef OAuth20CredentialsTest_INCLUDED
#define OAuth20CredentialsTest_INCLUDED


#include "Poco/Net/Net.h"
#include "CppUnit/TestCase.h"


class OAuth20CredentialsTest: public CppUnit::TestCase
{
public:
	OAuth20CredentialsTest(const std::string& name);
	~OAuth20CredentialsTest();

	void testAuthorize();
	void testAuthorizeCustomScheme();
	void testExtract();
	void testExtractCustomScheme();
	void testExtractNoCreds();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // OAuth20CredentialsTest_INCLUDED
