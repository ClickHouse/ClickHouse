//
// NetTestSuite.cpp
//
// $Id: //poco/1.4/Net/testsuite/src/NetTestSuite.cpp#2 $
//
// Copyright (c) 2005-2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "NetTestSuite.h"
#include "NetCoreTestSuite.h"
#include "SocketsTestSuite.h"
#include "MessagesTestSuite.h"
#include "HTTPTestSuite.h"
#include "HTTPClientTestSuite.h"
#include "TCPServerTestSuite.h"
#include "HTTPServerTestSuite.h"
#include "HTMLTestSuite.h"
#include "ReactorTestSuite.h"
#include "FTPClientTestSuite.h"
#include "MailTestSuite.h"
#include "ICMPClientTestSuite.h"
#include "NTPClientTestSuite.h"
#include "WebSocketTestSuite.h"
#include "OAuthTestSuite.h"
#include "SyslogTest.h"


CppUnit::Test* NetTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("NetTestSuite");

	pSuite->addTest(NetCoreTestSuite::suite());
	pSuite->addTest(SocketsTestSuite::suite());
	pSuite->addTest(MessagesTestSuite::suite());
	pSuite->addTest(HTTPTestSuite::suite());
	pSuite->addTest(HTTPClientTestSuite::suite());
	pSuite->addTest(TCPServerTestSuite::suite());
	pSuite->addTest(HTTPServerTestSuite::suite());
	pSuite->addTest(HTMLTestSuite::suite());
	pSuite->addTest(ReactorTestSuite::suite());
	pSuite->addTest(FTPClientTestSuite::suite());
	pSuite->addTest(MailTestSuite::suite());
	pSuite->addTest(ICMPClientTestSuite::suite());
	pSuite->addTest(NTPClientTestSuite::suite());
	pSuite->addTest(WebSocketTestSuite::suite());
	pSuite->addTest(OAuthTestSuite::suite());
	pSuite->addTest(SyslogTest::suite());

	return pSuite;
}
