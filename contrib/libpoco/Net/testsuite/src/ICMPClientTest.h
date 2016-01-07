//
// ICMPClientTest.h
//
// $Id: //poco/1.4/Net/testsuite/src/ICMPClientTest.h#1 $
//
// Definition of the ICMPClientTest class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef ICMPClientTest_INCLUDED
#define ICMPClientTest_INCLUDED


#include "Poco/Net/Net.h"
#include "CppUnit/TestCase.h"
#include "Poco/Net/ICMPClient.h"
#include "Poco/Net/ICMPEventArgs.h"


class ICMPClientTest: public CppUnit::TestCase
{
public:
	ICMPClientTest(const std::string& name);
	~ICMPClientTest();

	void testPing();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

	void onBegin(const void* pSender, Poco::Net::ICMPEventArgs& args);
	void onReply(const void* pSender, Poco::Net::ICMPEventArgs& args);
	void onError(const void* pSender, Poco::Net::ICMPEventArgs& args);
	void onEnd(const void* pSender, Poco::Net::ICMPEventArgs& args);

private:
	Poco::Net::ICMPClient _icmpClient;
};


#endif // ICMPClientTest_INCLUDED
