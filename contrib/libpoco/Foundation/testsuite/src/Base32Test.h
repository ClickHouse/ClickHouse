//
// Base32Test.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/Base32Test.h#1 $
//
// Definition of the Base32Test class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Base32Test_INCLUDED
#define Base32Test_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class Base32Test: public CppUnit::TestCase
{
public:
	Base32Test(const std::string& name);
	~Base32Test();

	void testEncoder();
	void testDecoder();
	void testEncodeDecode();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // Base32Test_INCLUDED
