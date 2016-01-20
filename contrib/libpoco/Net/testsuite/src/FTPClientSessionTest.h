//
// FTPClientSessionTest.h
//
// $Id: //poco/svn/Net/testsuite/src/FTPClientSessionTest.h#2 $
//
// Definition of the FTPClientSessionTest class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef FTPClientSessionTest_INCLUDED
#define FTPClientSessionTest_INCLUDED


#include "Poco/Net/Net.h"
#include "CppUnit/TestCase.h"


namespace Poco {
namespace Net {

class FTPClientSession;

} }

class DialogServer;

class FTPClientSessionTest: public CppUnit::TestCase
{
public:
	FTPClientSessionTest(const std::string& name);
	~FTPClientSessionTest();

	void testLogin1();
	void testLogin2();
	void testLogin3();
	void testLoginFailed1();
	void testLoginFailed2();
	void testCommands();
	void testDownloadPORT();
	void testDownloadEPRT();
	void testDownloadPASV();
	void testDownloadEPSV();
	void testUpload();
	void testList();
	
	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
	void login(DialogServer& server, Poco::Net::FTPClientSession& session);
};


#endif // FTPClientSessionTest_INCLUDED
