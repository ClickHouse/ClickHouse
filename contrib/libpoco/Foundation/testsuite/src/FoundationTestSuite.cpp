//
// FoundationTestSuite.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/FoundationTestSuite.cpp#3 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "FoundationTestSuite.h"
#include "CoreTestSuite.h"
#include "DateTimeTestSuite.h"
#include "StreamsTestSuite.h"
#include "CryptTestSuite.h"
#include "NotificationsTestSuite.h"
#include "ThreadingTestSuite.h"
#include "SharedLibraryTestSuite.h"
#include "LoggingTestSuite.h"
#include "FilesystemTestSuite.h"
#include "UUIDTestSuite.h"
#include "TextTestSuite.h"
#include "URITestSuite.h"
#if !defined(POCO_VXWORKS)
#include "ProcessesTestSuite.h"
#endif
#include "TaskTestSuite.h"
#include "EventTestSuite.h"
#include "CacheTestSuite.h"
#include "HashingTestSuite.h"


CppUnit::Test* FoundationTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("FoundationTestSuite");

	pSuite->addTest(CoreTestSuite::suite());
	pSuite->addTest(DateTimeTestSuite::suite());
	pSuite->addTest(StreamsTestSuite::suite());
	pSuite->addTest(CryptTestSuite::suite());
	pSuite->addTest(NotificationsTestSuite::suite());
	pSuite->addTest(ThreadingTestSuite::suite());
	pSuite->addTest(SharedLibraryTestSuite::suite());
	pSuite->addTest(LoggingTestSuite::suite());
	pSuite->addTest(FilesystemTestSuite::suite());
	pSuite->addTest(UUIDTestSuite::suite());
	pSuite->addTest(TextTestSuite::suite());
	pSuite->addTest(URITestSuite::suite());
#if !defined(POCO_VXWORKS)
	pSuite->addTest(ProcessesTestSuite::suite());
#endif
	pSuite->addTest(TaskTestSuite::suite());
	pSuite->addTest(EventTestSuite::suite());
	pSuite->addTest(CacheTestSuite::suite());
	pSuite->addTest(HashingTestSuite::suite());

	return pSuite;
}
