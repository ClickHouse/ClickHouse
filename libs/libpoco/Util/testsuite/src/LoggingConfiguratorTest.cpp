//
// LoggingConfiguratorTest.cpp
//
// $Id: //poco/1.4/Util/testsuite/src/LoggingConfiguratorTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "LoggingConfiguratorTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Util/LoggingConfigurator.h"
#include "Poco/Util/PropertyFileConfiguration.h"
#include "Poco/LoggingRegistry.h"
#include "Poco/ConsoleChannel.h"
#if defined(_WIN32)
#include "Poco/WindowsConsoleChannel.h"
#endif
#include "Poco/FileChannel.h"
#include "Poco/SplitterChannel.h"
#include "Poco/FormattingChannel.h"
#include "Poco/PatternFormatter.h"
#include "Poco/Logger.h"
#include "Poco/Message.h"
#include "Poco/AutoPtr.h"
#include <sstream>


using Poco::Util::LoggingConfigurator;
using Poco::Util::PropertyFileConfiguration;
using Poco::LoggingRegistry;
using Poco::Channel;
using Poco::ConsoleChannel;
using Poco::FileChannel;
using Poco::SplitterChannel;
using Poco::FormattingChannel;
using Poco::PatternFormatter;
using Poco::Logger;
using Poco::Message;
using Poco::AutoPtr;


LoggingConfiguratorTest::LoggingConfiguratorTest(const std::string& name): CppUnit::TestCase(name)
{
}


LoggingConfiguratorTest::~LoggingConfiguratorTest()
{
}


void LoggingConfiguratorTest::testConfigurator()
{
	static const std::string config =
		"logging.loggers.root.channel = c1\n"
		"logging.loggers.root.level = warning\n"
		"logging.loggers.l1.name = logger1\n"
		"logging.loggers.l1.channel.class = FileChannel\n"
		"logging.loggers.l1.channel.pattern = %s: [%p] %t\n"
		"logging.loggers.l1.channel.path = logfile.log\n"
		"logging.loggers.l1.level = information\n"
		"logging.loggers.l2.name = logger2\n"
		"logging.loggers.l2.channel.class = SplitterChannel\n"
		"logging.loggers.l2.channel.channel1 = c2\n"
		"logging.loggers.l2.channel.channel2 = c3\n"
		"logging.loggers.l2.level = debug\n"
		"logging.channels.c1.class = ConsoleChannel\n"
		"logging.channels.c1.formatter = f1\n"
		"logging.channels.c2.class = FileChannel\n"
		"logging.channels.c2.path = ${system.tempDir}/sample.log\n"
		"logging.channels.c2.formatter.class = PatternFormatter\n"
		"logging.channels.c2.formatter.pattern = %s: {%p} %t\n"
		"logging.channels.c3.class = ConsoleChannel\n"
		"logging.channels.c3.pattern = %s: [%p] %t\n"
		"logging.formatters.f1.class = PatternFormatter\n"
		"logging.formatters.f1.pattern = %s-[%p] %t\n"
		"logging.formatters.f1.times = UTC\n";

	std::istringstream istr(config);
	AutoPtr<PropertyFileConfiguration> pConfig = new PropertyFileConfiguration(istr);

	LoggingConfigurator configurator;
	configurator.configure(pConfig);
	
	Logger& root = Logger::get("");
	assert (root.getLevel() == Message::PRIO_WARNING);
	FormattingChannel* pFC = dynamic_cast<FormattingChannel*>(root.getChannel());
	assertNotNull (pFC);
#if defined(_WIN32) && !defined(_WIN32_WCE)
	assertNotNull (dynamic_cast<Poco::WindowsConsoleChannel*>(pFC->getChannel()));
#else
	assertNotNull (dynamic_cast<ConsoleChannel*>(pFC->getChannel()));
#endif
	assertNotNull (dynamic_cast<PatternFormatter*>(pFC->getFormatter()));
	assert (static_cast<PatternFormatter*>(pFC->getFormatter())->getProperty("pattern") == "%s-[%p] %t");
	
	Logger& logger1 = Logger::get("logger1");
	assert (logger1.getLevel() == Message::PRIO_INFORMATION);
	pFC = dynamic_cast<FormattingChannel*>(logger1.getChannel());
	assertNotNull (pFC);
	assertNotNull (dynamic_cast<FileChannel*>(pFC->getChannel()));
	assertNotNull (dynamic_cast<PatternFormatter*>(pFC->getFormatter()));
	assert (static_cast<PatternFormatter*>(pFC->getFormatter())->getProperty("pattern") == "%s: [%p] %t");

	Logger& logger2 = Logger::get("logger2");
	assert (logger2.getLevel() == Message::PRIO_DEBUG);
	assertNotNull (dynamic_cast<SplitterChannel*>(logger2.getChannel()));	
}


void LoggingConfiguratorTest::testBadConfiguration1()
{
	// this is mainly testing for memory leaks in case of 
	// a bad configuration.
	
	static const std::string config =
		"logging.loggers.root.channel = c1\n"
		"logging.loggers.root.level = warning\n"
		"logging.loggers.l1.name = logger1\n"
		"logging.loggers.l1.channel.class = FileChannel\n"
		"logging.loggers.l1.channel.pattern = %s: [%p] %t\n"
		"logging.loggers.l1.channel.path = logfile.log\n"
		"logging.loggers.l1.level = information\n"
		"logging.loggers.l2.name = logger2\n"
		"logging.loggers.l2.channel.class = UnknownChannel\n"
		"logging.loggers.l2.level = debug\n"
		"logging.channels.c1.class = ConsoleChannel\n"
		"logging.channels.c1.formatter = f1\n"
		"logging.channels.c2.class = FileChannel\n"
		"logging.channels.c2.path = ${system.tempDir}/sample.log\n"
		"logging.channels.c2.formatter.class = PatternFormatter\n"
		"logging.channels.c2.formatter.pattern = %s: {%p} %t\n"
		"logging.channels.c3.class = ConsoleChannel\n"
		"logging.channels.c3.pattern = %s: [%p] %t\n"
		"logging.formatters.f1.class = PatternFormatter\n"
		"logging.formatters.f1.pattern = %s-[%p] %t\n"
		"logging.formatters.f1.times = UTC\n";

	std::istringstream istr(config);
	AutoPtr<PropertyFileConfiguration> pConfig = new PropertyFileConfiguration(istr);

	LoggingConfigurator configurator;
	try
	{
		configurator.configure(pConfig);
		fail("bad configuration - must throw");
	}
	catch (Poco::Exception&)
	{
	}
}


void LoggingConfiguratorTest::testBadConfiguration2()
{
	// this is mainly testing for memory leaks in case of 
	// a bad configuration.
	
	static const std::string config =
		"logging.loggers.root.channel = c1\n"
		"logging.loggers.root.level = unknown\n"
		"logging.loggers.l1.name = logger1\n"
		"logging.loggers.l1.channel.class = FileChannel\n"
		"logging.loggers.l1.channel.pattern = %s: [%p] %t\n"
		"logging.loggers.l1.channel.path = logfile.log\n"
		"logging.loggers.l1.level = information\n"
		"logging.loggers.l2.name = logger2\n"
		"logging.loggers.l2.channel.class = SplitterChannel\n"
		"logging.loggers.l2.level = debug\n"
		"logging.channels.c1.class = ConsoleChannel\n"
		"logging.channels.c1.formatter = f1\n"
		"logging.channels.c2.class = FileChannel\n"
		"logging.channels.c2.path = ${system.tempDir}/sample.log\n"
		"logging.channels.c2.formatter.class = PatternFormatter\n"
		"logging.channels.c2.formatter.pattern = %s: {%p} %t\n"
		"logging.channels.c3.class = ConsoleChannel\n"
		"logging.channels.c3.pattern = %s: [%p] %t\n"
		"logging.formatters.f1.class = PatternFormatter\n"
		"logging.formatters.f1.pattern = %s-[%p] %t\n"
		"logging.formatters.f1.times = UTC\n";

	std::istringstream istr(config);
	AutoPtr<PropertyFileConfiguration> pConfig = new PropertyFileConfiguration(istr);

	LoggingConfigurator configurator;
	try
	{
		configurator.configure(pConfig);
		fail("bad configuration - must throw");
	}
	catch (Poco::Exception&)
	{
	}
}


void LoggingConfiguratorTest::testBadConfiguration3()
{
	// this is mainly testing for memory leaks in case of 
	// a bad configuration.
	
	static const std::string config =
		"logging.loggers.root.channel = c1\n"
		"logging.loggers.root.level = warning\n"
		"logging.loggers.l1.name = logger1\n"
		"logging.loggers.l1.channel.class = FileChannel\n"
		"logging.loggers.l1.channel.pattern = %s: [%p] %t\n"
		"logging.loggers.l1.channel.path = logfile.log\n"
		"logging.loggers.l1.level = information\n"
		"logging.loggers.l2.name = logger2\n"
		"logging.loggers.l2.channel.class = UnknownChannel\n"
		"logging.loggers.l2.level = debug\n"
		"logging.channels.c1.class = ConsoleChannel\n"
		"logging.channels.c1.formatter = f1\n"
		"logging.channels.c2.class = FileChannel\n"
		"logging.channels.c2.path = ${system.tempDir}/sample.log\n"
		"logging.channels.c2.formatter.class = UnknownFormatter\n"
		"logging.channels.c2.formatter.pattern = %s: {%p} %t\n"
		"logging.channels.c3.class = ConsoleChannel\n"
		"logging.channels.c3.pattern = %s: [%p] %t\n"
		"logging.formatters.f1.class = PatternFormatter\n"
		"logging.formatters.f1.pattern = %s-[%p] %t\n"
		"logging.formatters.f1.times = UTC\n";

	std::istringstream istr(config);
	AutoPtr<PropertyFileConfiguration> pConfig = new PropertyFileConfiguration(istr);

	LoggingConfigurator configurator;
	try
	{
		configurator.configure(pConfig);
		fail("bad configuration - must throw");
	}
	catch (Poco::Exception&)
	{
	}
}


void LoggingConfiguratorTest::testBadConfiguration4()
{
	// this is mainly testing for memory leaks in case of 
	// a bad configuration.
	
	static const std::string config =
		"logging.loggers.root.channel = c1\n"
		"logging.loggers.root.level = warning\n"
		"logging.loggers.l1.name = logger1\n"
		"logging.loggers.l1.channel.class = FileChannel\n"
		"logging.loggers.l1.channel.pattern = %s: [%p] %t\n"
		"logging.loggers.l1.channel.path = logfile.log\n"
		"logging.loggers.l1.level = information\n"
		"logging.loggers.l2.name = logger2\n"
		"logging.loggers.l2.channel.class = UnknownChannel\n"
		"logging.loggers.l2.level = debug\n"
		"logging.channels.c1.class = ConsoleChannel\n"
		"logging.channels.c1.formatter = f1\n"
		"logging.channels.c2.class = FileChannel\n"
		"logging.channels.c2.path = ${system.tempDir}/sample.log\n"
		"logging.channels.c2.formatter.class = PatternFormatter\n"
		"logging.channels.c2.formatter.pattern = %s: {%p} %t\n"
		"logging.channels.c2.formatter.unknown = FOO\n"
		"logging.channels.c3.class = ConsoleChannel\n"
		"logging.channels.c3.pattern = %s: [%p] %t\n"
		"logging.formatters.f1.class = PatternFormatter\n"
		"logging.formatters.f1.pattern = %s-[%p] %t\n"
		"logging.formatters.f1.times = UTC\n";

	std::istringstream istr(config);
	AutoPtr<PropertyFileConfiguration> pConfig = new PropertyFileConfiguration(istr);

	LoggingConfigurator configurator;
	try
	{
		configurator.configure(pConfig);
		fail("bad configuration - must throw");
	}
	catch (Poco::Exception&)
	{
	}
}


void LoggingConfiguratorTest::setUp()
{
	LoggingRegistry::defaultRegistry().clear();
}


void LoggingConfiguratorTest::tearDown()
{
}


CppUnit::Test* LoggingConfiguratorTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("LoggingConfiguratorTest");

	CppUnit_addTest(pSuite, LoggingConfiguratorTest, testConfigurator);
	CppUnit_addTest(pSuite, LoggingConfiguratorTest, testBadConfiguration1);
	CppUnit_addTest(pSuite, LoggingConfiguratorTest, testBadConfiguration2);
	CppUnit_addTest(pSuite, LoggingConfiguratorTest, testBadConfiguration3);
	CppUnit_addTest(pSuite, LoggingConfiguratorTest, testBadConfiguration4);

	return pSuite;
}
