//
// FTPStreamFactoryTest.h
//
// $Id: //poco/1.4/Net/testsuite/src/FTPStreamFactoryTest.h#1 $
//
// Definition of the FTPStreamFactoryTest class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef FTPStreamFactoryTest_INCLUDED
#define FTPStreamFactoryTest_INCLUDED


#include "Poco/Net/Net.h"
#include "CppUnit/TestCase.h"


class FTPStreamFactoryTest: public CppUnit::TestCase
{
public:
	FTPStreamFactoryTest(const std::string& name);
	~FTPStreamFactoryTest();

	void testDownload();
	void testList();
	void testUserInfo();
	void testPasswordProvider();
	void testMissingPasswordProvider();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // FTPStreamFactoryTest_INCLUDED
