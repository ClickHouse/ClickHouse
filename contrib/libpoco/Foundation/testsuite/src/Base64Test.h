//
// Base64Test.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/Base64Test.h#1 $
//
// Definition of the Base64Test class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Base64Test_INCLUDED
#define Base64Test_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class Base64Test: public CppUnit::TestCase
{
public:
	Base64Test(const std::string& name);
	~Base64Test();

	void testEncoder();
	void testDecoder();
	void testEncodeDecode();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // Base64Test_INCLUDED
