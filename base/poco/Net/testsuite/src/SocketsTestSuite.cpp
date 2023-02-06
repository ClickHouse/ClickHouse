//
// SocketsTestSuite.cpp
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "SocketsTestSuite.h"
#include "SocketTest.h"
#include "SocketStreamTest.h"
#include "DatagramSocketTest.h"
#include "MulticastSocketTest.h"
#include "DialogSocketTest.h"
#include "RawSocketTest.h"
#include "PollSetTest.h"


CppUnit::Test* SocketsTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("SocketsTestSuite");

	pSuite->addTest(SocketTest::suite());
	pSuite->addTest(SocketStreamTest::suite());
	pSuite->addTest(DatagramSocketTest::suite());
	pSuite->addTest(DialogSocketTest::suite());
	pSuite->addTest(RawSocketTest::suite());
#ifdef POCO_NET_HAS_INTERFACE
	pSuite->addTest(MulticastSocketTest::suite());
#endif
	pSuite->addTest(PollSetTest::suite());
	return pSuite;
}
