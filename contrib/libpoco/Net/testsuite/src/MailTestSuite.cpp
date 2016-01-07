//
// MailTestSuite.cpp
//
// $Id: //poco/1.4/Net/testsuite/src/MailTestSuite.cpp#1 $
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "MailTestSuite.h"
#include "MailMessageTest.h"
#include "MailStreamTest.h"
#include "SMTPClientSessionTest.h"
#include "POP3ClientSessionTest.h"


CppUnit::Test* MailTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("MailTestSuite");

	pSuite->addTest(MailMessageTest::suite());
	pSuite->addTest(MailStreamTest::suite());
	pSuite->addTest(SMTPClientSessionTest::suite());
	pSuite->addTest(POP3ClientSessionTest::suite());

	return pSuite;
}
