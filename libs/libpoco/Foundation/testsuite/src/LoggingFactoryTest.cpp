//
// LoggingFactoryTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/LoggingFactoryTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "LoggingFactoryTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/LoggingFactory.h"
#include "Poco/Instantiator.h"
#include "Poco/Channel.h"
#include "Poco/ConsoleChannel.h"
#if defined(_WIN32)
#include "Poco/WindowsConsoleChannel.h"
#endif
#include "Poco/FileChannel.h"
#include "Poco/SplitterChannel.h"
#include "Poco/Formatter.h"
#include "Poco/PatternFormatter.h"
#include "Poco/Message.h"
#include "Poco/AutoPtr.h"
#include "Poco/Exception.h"
#include <memory>


using Poco::LoggingFactory;
using Poco::Channel;
using Poco::ConsoleChannel;
using Poco::FileChannel;
using Poco::SplitterChannel;
using Poco::Formatter;
using Poco::PatternFormatter;
using Poco::Message;
using Poco::AutoPtr;
using Poco::Instantiator;


namespace
{
	class CustomChannel: public Channel
	{
	public:
		void log(const Message& msg)
		{
		}
	};
	
	class CustomFormatter: public Formatter
	{
		void format(const Message& msg, std::string& text)
		{
		}
	};
}


LoggingFactoryTest::LoggingFactoryTest(const std::string& name): CppUnit::TestCase(name)
{
}


LoggingFactoryTest::~LoggingFactoryTest()
{
}


void LoggingFactoryTest::testBuiltins()
{
	LoggingFactory& fact = LoggingFactory::defaultFactory();
	
	AutoPtr<Channel> pConsoleChannel = fact.createChannel("ConsoleChannel");
#if defined(_WIN32) && !defined(_WIN32_WCE)
	assert (dynamic_cast<Poco::WindowsConsoleChannel*>(pConsoleChannel.get()) != 0);
#else
	assert (dynamic_cast<ConsoleChannel*>(pConsoleChannel.get()) != 0);
#endif

	AutoPtr<Channel> pFileChannel = fact.createChannel("FileChannel");
	assert (dynamic_cast<FileChannel*>(pFileChannel.get()) != 0);
	
	AutoPtr<Channel> pSplitterChannel = fact.createChannel("SplitterChannel");
	assert (dynamic_cast<SplitterChannel*>(pSplitterChannel.get()) != 0);
	
	try
	{
		AutoPtr<Channel> pUnknownChannel = fact.createChannel("UnknownChannel");
		fail("unknown class - must throw");
	}
	catch (Poco::NotFoundException&)
	{
	}
	
	AutoPtr<Formatter> pPatternFormatter = fact.createFormatter("PatternFormatter");
	assert (dynamic_cast<PatternFormatter*>(pPatternFormatter.get()) != 0);
	
	try
	{
		AutoPtr<Formatter> pUnknownFormatter = fact.createFormatter("UnknownFormatter");
		fail("unknown class - must throw");
	}
	catch (Poco::NotFoundException&)
	{
	}
}


void LoggingFactoryTest::testCustom()
{
	std::auto_ptr<LoggingFactory> fact(new LoggingFactory);
	
	fact->registerChannelClass("CustomChannel", new Instantiator<CustomChannel, Channel>);
	fact->registerFormatterClass("CustomFormatter", new Instantiator<CustomFormatter, Formatter>);

	AutoPtr<Channel> pCustomChannel = fact->createChannel("CustomChannel");
	assert (dynamic_cast<CustomChannel*>(pCustomChannel.get()) != 0);

	AutoPtr<Formatter> pCustomFormatter = fact->createFormatter("CustomFormatter");
	assert (dynamic_cast<CustomFormatter*>(pCustomFormatter.get()) != 0);
}


void LoggingFactoryTest::setUp()
{
}


void LoggingFactoryTest::tearDown()
{
}


CppUnit::Test* LoggingFactoryTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("LoggingFactoryTest");

	CppUnit_addTest(pSuite, LoggingFactoryTest, testBuiltins);
	CppUnit_addTest(pSuite, LoggingFactoryTest, testCustom);

	return pSuite;
}
