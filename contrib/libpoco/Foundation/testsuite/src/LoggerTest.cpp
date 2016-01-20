//
// LoggerTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/LoggerTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "LoggerTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Logger.h"
#include "Poco/AutoPtr.h"
#include "TestChannel.h"


using Poco::Logger;
using Poco::Channel;
using Poco::Message;
using Poco::AutoPtr;


LoggerTest::LoggerTest(const std::string& name): CppUnit::TestCase(name)
{
}


LoggerTest::~LoggerTest()
{
}


void LoggerTest::testLogger()
{
	AutoPtr<TestChannel> pChannel = new TestChannel;
	Logger& root = Logger::root();
	root.setChannel(pChannel.get());
	assert (root.getLevel() == Message::PRIO_INFORMATION);
	assert (root.is(Message::PRIO_INFORMATION));
	assert (root.fatal());
	assert (root.critical());
	assert (root.error());
	assert (root.warning());
	assert (root.notice());
	assert (root.information());
	assert (!root.debug());
	assert (!root.trace());
	
	root.information("Informational message");
	assert (pChannel->list().size() == 1);
	root.warning("Warning message");
	assert (pChannel->list().size() == 2);
	root.debug("Debug message");
	assert (pChannel->list().size() == 2);
	
	Logger& logger1 = Logger::get("Logger1");
	Logger& logger2 = Logger::get("Logger2");
	Logger& logger11 = Logger::get("Logger1.Logger1");
	Logger& logger12 = Logger::get("Logger1.Logger2");
	Logger& logger21 = Logger::get("Logger2.Logger1");
	Logger& logger22 = Logger::get("Logger2.Logger2");

	std::vector<std::string> loggers;
	Logger::names(loggers);
	assert (loggers.size() == 7);
	assert (loggers[0] == "");
	assert (loggers[1] == "Logger1");
	assert (loggers[2] == "Logger1.Logger1");
	assert (loggers[3] == "Logger1.Logger2");
	assert (loggers[4] == "Logger2");
	assert (loggers[5] == "Logger2.Logger1");
	assert (loggers[6] == "Logger2.Logger2");

	Logger::setLevel("Logger1", Message::PRIO_DEBUG);
	assert (logger1.is(Message::PRIO_DEBUG));
	assert (logger11.is(Message::PRIO_DEBUG));
	assert (logger12.is(Message::PRIO_DEBUG));
	assert (!logger2.is(Message::PRIO_DEBUG));
	assert (!logger21.is(Message::PRIO_DEBUG));
	assert (!logger22.is(Message::PRIO_DEBUG));
	assert (logger11.is(Message::PRIO_INFORMATION));
	assert (logger12.is(Message::PRIO_INFORMATION));
	assert (logger21.is(Message::PRIO_INFORMATION));
	assert (logger22.is(Message::PRIO_INFORMATION));
	
	Logger::setLevel("Logger2.Logger1", Message::PRIO_ERROR);
	assert (logger1.is(Message::PRIO_DEBUG));
	assert (logger11.is(Message::PRIO_DEBUG));
	assert (logger12.is(Message::PRIO_DEBUG));
	assert (!logger21.is(Message::PRIO_DEBUG));
	assert (!logger22.is(Message::PRIO_DEBUG));
	assert (logger11.is(Message::PRIO_INFORMATION));
	assert (logger12.is(Message::PRIO_INFORMATION));
	assert (logger21.is(Message::PRIO_ERROR));
	assert (logger22.is(Message::PRIO_INFORMATION));
	
	Logger::setLevel("", Message::PRIO_WARNING);
	assert (root.getLevel() == Message::PRIO_WARNING);
	assert (logger1.getLevel() == Message::PRIO_WARNING);
	assert (logger11.getLevel() == Message::PRIO_WARNING);
	assert (logger12.getLevel() == Message::PRIO_WARNING);
	assert (logger1.getLevel() == Message::PRIO_WARNING);
	assert (logger21.getLevel() == Message::PRIO_WARNING);
	assert (logger22.getLevel() == Message::PRIO_WARNING);
	
	AutoPtr<TestChannel> pChannel2 = new TestChannel;
	Logger::setChannel("Logger2", pChannel2.get());
	assert (pChannel  == root.getChannel());
	assert (pChannel  == logger1.getChannel());
	assert (pChannel  == logger11.getChannel());
	assert (pChannel  == logger12.getChannel());
	assert (pChannel2 == logger2.getChannel());
	assert (pChannel2 == logger21.getChannel());
	assert (pChannel2 == logger22.getChannel());
	
	root.setLevel(Message::PRIO_TRACE);
	pChannel->list().clear();
	root.trace("trace");
	assert (pChannel->list().begin()->getPriority() == Message::PRIO_TRACE);
	pChannel->list().clear();
	root.debug("debug");
	assert (pChannel->list().begin()->getPriority() == Message::PRIO_DEBUG);
	pChannel->list().clear();
	root.information("information");
	assert (pChannel->list().begin()->getPriority() == Message::PRIO_INFORMATION);
	pChannel->list().clear();
	root.notice("notice");
	assert (pChannel->list().begin()->getPriority() == Message::PRIO_NOTICE);
	pChannel->list().clear();
	root.warning("warning");
	assert (pChannel->list().begin()->getPriority() == Message::PRIO_WARNING);
	pChannel->list().clear();
	root.error("error");
	assert (pChannel->list().begin()->getPriority() == Message::PRIO_ERROR);
	pChannel->list().clear();
	root.critical("critical");
	assert (pChannel->list().begin()->getPriority() == Message::PRIO_CRITICAL);
	pChannel->list().clear();
	root.fatal("fatal");
	assert (pChannel->list().begin()->getPriority() == Message::PRIO_FATAL);
	
	root.setLevel("1");
	assert (root.getLevel() == Message::PRIO_FATAL);
	root.setLevel("8");
	assert (root.getLevel() == Message::PRIO_TRACE);
	try
	{
		root.setLevel("0");
		assert(0);
	}
	catch(Poco::InvalidArgumentException&)
	{
	}
	try
	{
		root.setLevel("9");
		assert(0);
	}
	catch(Poco::InvalidArgumentException&)
	{
	}

}


void LoggerTest::testFormat()
{
	std::string str = Logger::format("$0$1", "foo", "bar");
	assert (str == "foobar");
	str = Logger::format("foo$0", "bar");
	assert (str == "foobar");
	str = Logger::format("the amount is $$ $0", "100");
	assert (str == "the amount is $ 100");
	str = Logger::format("$0$1$2", "foo", "bar");
	assert (str == "foobar");
	str = Logger::format("$foo$0", "bar");
	assert (str == "$foobar");
	str = Logger::format("$0", "1");
	assert (str == "1");
	str = Logger::format("$0$1", "1", "2");
	assert (str == "12");
	str = Logger::format("$0$1$2", "1", "2", "3");
	assert (str == "123");
	str = Logger::format("$0$1$2$3", "1", "2", "3", "4");
	assert (str == "1234");
}

void LoggerTest::testFormatAny()
{
	AutoPtr<TestChannel> pChannel = new TestChannel;
	Logger& root = Logger::root();
	root.setChannel(pChannel.get());

	root.error("%s%s", std::string("foo"), std::string("bar"));
	assert (pChannel->getLastMessage().getText() == "foobar");

	root.error("foo%s", std::string("bar"));
	assert (pChannel->getLastMessage().getText() == "foobar");

	root.error("the amount is %% %d", 100);
	assert (pChannel->getLastMessage().getText() == "the amount is % 100");

	root.error("%d", 1);
	assert (pChannel->getLastMessage().getText() == "1");

	root.error("%d%d", 1, 2);
	assert (pChannel->getLastMessage().getText() == "12");

	root.error("%d%d%d", 1, 2, 3);
	assert (pChannel->getLastMessage().getText() == "123");

	root.error("%d%d%d%d", 1, 2, 3, 4);
	assert (pChannel->getLastMessage().getText() == "1234");

	root.error("%d%d%d%d%d", 1, 2, 3, 4, 5);
	assert (pChannel->getLastMessage().getText() == "12345");

	root.error("%d%d%d%d%d%d", 1, 2, 3, 4, 5, 6);
	assert (pChannel->getLastMessage().getText() == "123456");

	root.error("%d%d%d%d%d%d%d", 1, 2, 3, 4, 5, 6, 7);
	assert(pChannel->getLastMessage().getText() == "1234567");

	root.error("%d%d%d%d%d%d%d%d", 1, 2, 3, 4, 5, 6, 7, 8);
	assert(pChannel->getLastMessage().getText() == "12345678");

	root.error("%d%d%d%d%d%d%d%d%d", 1, 2, 3, 4, 5, 6, 7, 8, 9);
	assert(pChannel->getLastMessage().getText() == "123456789");

	root.error("%d%d%d%d%d%d%d%d%d%d", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
	assert(pChannel->getLastMessage().getText() == "12345678910");
}


void LoggerTest::testDump()
{
	AutoPtr<TestChannel> pChannel = new TestChannel;
	Logger& root = Logger::root();
	root.setChannel(pChannel.get());
	root.setLevel(Message::PRIO_INFORMATION);
	
	char buffer1[] = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05};
	root.dump("test", buffer1, sizeof(buffer1));
	assert (pChannel->list().empty());
	
	root.setLevel(Message::PRIO_DEBUG);
	root.dump("test", buffer1, sizeof(buffer1));
	
	std::string msg = pChannel->list().begin()->getText();
	assert (msg == "test\n0000  00 01 02 03 04 05                                 ......");
	pChannel->clear();
	
	char buffer2[] = {
		0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
		0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f
	};
	root.dump("", buffer2, sizeof(buffer2));
	msg = pChannel->list().begin()->getText();
	assert (msg == "0000  00 01 02 03 04 05 06 07  08 09 0A 0B 0C 0D 0E 0F  ................");
	pChannel->clear();
	
	char buffer3[] = {
		0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
		0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
		0x20, 0x41, 0x42, 0x1f, 0x7f, 0x7e
	};
	root.dump("", buffer3, sizeof(buffer3));
	msg = pChannel->list().begin()->getText();
	assert (msg == "0000  00 01 02 03 04 05 06 07  08 09 0A 0B 0C 0D 0E 0F  ................\n"
	               "0010  20 41 42 1F 7F 7E                                  AB..~");
	pChannel->clear();
}


void LoggerTest::setUp()
{
	Logger::shutdown();
}


void LoggerTest::tearDown()
{
}


CppUnit::Test* LoggerTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("LoggerTest");

	CppUnit_addTest(pSuite, LoggerTest, testLogger);
	CppUnit_addTest(pSuite, LoggerTest, testFormat);
	CppUnit_addTest(pSuite, LoggerTest, testFormatAny);
	CppUnit_addTest(pSuite, LoggerTest, testDump);

	return pSuite;
}
