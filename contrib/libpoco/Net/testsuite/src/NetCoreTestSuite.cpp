//
// NetCoreTestSuite.cpp
//
// $Id: //poco/1.4/Net/testsuite/src/NetCoreTestSuite.cpp#1 $
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "NetCoreTestSuite.h"
#include "IPAddressTest.h"
#include "SocketAddressTest.h"
#include "DNSTest.h"
#include "NetworkInterfaceTest.h"


CppUnit::Test* NetCoreTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("NetCoreTestSuite");

	pSuite->addTest(IPAddressTest::suite());
	pSuite->addTest(SocketAddressTest::suite());
	pSuite->addTest(DNSTest::suite());
#ifdef POCO_NET_HAS_INTERFACE
	pSuite->addTest(NetworkInterfaceTest::suite());
#endif // POCO_NET_HAS_INTERFACE
	return pSuite;
}
