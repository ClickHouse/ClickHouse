//
// MailStreamTest.cpp
//
// $Id: //poco/1.4/Net/testsuite/src/MailStreamTest.cpp#1 $
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "MailStreamTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Net/MailStream.h"
#include "Poco/StreamCopier.h"
#include <sstream>


using Poco::Net::MailInputStream;
using Poco::Net::MailOutputStream;
using Poco::StreamCopier;


MailStreamTest::MailStreamTest(const std::string& name): CppUnit::TestCase(name)
{
}


MailStreamTest::~MailStreamTest()
{
}


void MailStreamTest::testMailInputStream()
{
	std::istringstream istr(
		"From: john.doe@no.domain\r\n"
		"To: jane.doe@no.domain\r\n"
		"Subject: test\r\n"
		"\r\n"
		"This is a test.\r\n"
		"\rThis.is.\ngarbage\r.\r\n"
		".This line starts with a period.\r\n"
		"..and this one too\r\n"
		"..\r\n"
		".\r\n"
	);
	
	MailInputStream mis(istr);
	std::ostringstream ostr;
	StreamCopier::copyStream(mis, ostr);
	std::string s(ostr.str());
	assert (s ==
		"From: john.doe@no.domain\r\n"
		"To: jane.doe@no.domain\r\n"
		"Subject: test\r\n"
		"\r\n"
		"This is a test.\r\n"
		"\rThis.is.\ngarbage\r.\r\n"
		".This line starts with a period.\r\n"
		".and this one too\r\n"
		".\r\n"
	);	
}


void MailStreamTest::testMailOutputStream()
{
	std::string msg(
		"From: john.doe@no.domain\r\n"
		"To: jane.doe@no.domain\r\n"
		"Subject: test\r\n"
		"\r\n"
		"This is a test.\r\n"
		"\rThis.is.\ngarbage\r.\r\n"
		".This line starts with a period.\r\n"
		"\r\n"
		".and this one too\r\n"
		".\r\n"
	);
	
	std::ostringstream ostr;
	MailOutputStream mos(ostr);
	mos << msg;
	mos.close();
	std::string s(ostr.str());
	assert (s == 
		"From: john.doe@no.domain\r\n"
		"To: jane.doe@no.domain\r\n"
		"Subject: test\r\n"
		"\r\n"
		"This is a test.\r\n"
		"\rThis.is.\ngarbage\r.\r\n"
		"..This line starts with a period.\r\n"
		"\r\n"
		"..and this one too\r\n"
		"..\r\n"
		".\r\n"
	);
}


void MailStreamTest::setUp()
{
}


void MailStreamTest::tearDown()
{
}


CppUnit::Test* MailStreamTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("MailStreamTest");

	CppUnit_addTest(pSuite, MailStreamTest, testMailInputStream);
	CppUnit_addTest(pSuite, MailStreamTest, testMailOutputStream);

	return pSuite;
}
