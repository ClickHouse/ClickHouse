//
// LogStreamTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/LogStreamTest.cpp#1 $
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// All rights reserved.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "LogStreamTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Logger.h"
#include "Poco/LogStream.h"
#include "Poco/AutoPtr.h"
#include "TestChannel.h"


using Poco::Logger;
using Poco::LogStream;
using Poco::Channel;
using Poco::Message;
using Poco::AutoPtr;


LogStreamTest::LogStreamTest(const std::string& name): CppUnit::TestCase(name)
{
}


LogStreamTest::~LogStreamTest()
{
}


void LogStreamTest::testLogStream()
{
	AutoPtr<TestChannel> pChannel = new TestChannel;
	Logger& root = Logger::root();
	root.setChannel(pChannel.get());

	LogStream ls(root);

	ls << "information" << ' ' << 1 << std::endl;
	assert (pChannel->list().begin()->getPriority() == Message::PRIO_INFORMATION);
	assert (pChannel->list().begin()->getText() == "information 1");
	pChannel->list().clear();

	ls.error() << "error" << std::endl;
	assert (pChannel->list().begin()->getPriority() == Message::PRIO_ERROR);
	assert (pChannel->list().begin()->getText() == "error");
	pChannel->list().clear();
}


void LogStreamTest::setUp()
{
}


void LogStreamTest::tearDown()
{
}


CppUnit::Test* LogStreamTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("LogStreamTest");

	CppUnit_addTest(pSuite, LogStreamTest, testLogStream);

	return pSuite;
}
