//
// NTPClientTest.h
//
// $Id: //poco/1.4/Net/testsuite/src/NTPClientTest.h#1 $
//
// Definition of the NTPClientTest class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NTPClientTest_INCLUDED
#define NTPClientTest_INCLUDED


#include "Poco/Net/Net.h"
#include "CppUnit/TestCase.h"
#include "Poco/Net/NTPClient.h"
#include "Poco/Net/NTPEventArgs.h"


class NTPClientTest: public CppUnit::TestCase
{
public:
	NTPClientTest(const std::string& name);
	~NTPClientTest();

	void testTimeSync();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

	void onResponse(const void* pSender, Poco::Net::NTPEventArgs& args);
private:
	Poco::Net::NTPClient _ntpClient;
};


#endif // NTPClientTest_INCLUDED
