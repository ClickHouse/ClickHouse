//
// SocketTest.h
//
// $Id: //poco/1.4/Net/testsuite/src/SocketTest.h#1 $
//
// Definition of the SocketTest class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef SocketTest_INCLUDED
#define SocketTest_INCLUDED


#include "Poco/Net/Net.h"
#include "CppUnit/TestCase.h"


class SocketTest: public CppUnit::TestCase
{
public:
	SocketTest(const std::string& name);
	~SocketTest();

	void testEcho();
	void testPoll();
	void testAvailable();
	void testFIFOBuffer();
	void testConnect();
	void testConnectRefused();
	void testConnectRefusedNB();
	void testNonBlocking();
	void testAddress();
	void testAssign();
	void testTimeout();
	void testBufferSize();
	void testOptions();
	void testSelect();
	void testSelect2();
	void testSelect3();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
	void onReadable(bool& b);
	void onWritable(bool& b);

	int _readableToNot;
	int _notToReadable;
	int _writableToNot;
	int _notToWritable;
};


#endif // SocketTest_INCLUDED
