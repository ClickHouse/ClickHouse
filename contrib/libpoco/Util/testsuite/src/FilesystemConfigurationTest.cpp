//
// FilesystemConfigurationTest.cpp
//
// $Id: //poco/1.4/Util/testsuite/src/FilesystemConfigurationTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "FilesystemConfigurationTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Util/FilesystemConfiguration.h"
#include "Poco/AutoPtr.h"
#include "Poco/File.h"
#include <algorithm>


using Poco::Util::FilesystemConfiguration;
using Poco::Util::AbstractConfiguration;
using Poco::AutoPtr;


FilesystemConfigurationTest::FilesystemConfigurationTest(const std::string& name): AbstractConfigurationTest(name),
	_path("TestConfiguration")
{
}


FilesystemConfigurationTest::~FilesystemConfigurationTest()
{
}


void FilesystemConfigurationTest::testFilesystemConfiguration()
{
	AutoPtr<FilesystemConfiguration> config = new FilesystemConfiguration(_path.toString());
	
	config->setString("logging.loggers.root.channel.class", "ConsoleChannel");
	config->setString("logging.loggers.app.name", "Application");
	config->setString("logging.loggers.app.channel", "c1");
	config->setString("logging.formatters.f1.class", "PatternFormatter");
	config->setString("logging.formatters.f1.pattern", "[%p] %t");
	config->setString("logging.channels.c1.class", "ConsoleChannel");

	assert (config->getString("logging.loggers.root.channel.class") == "ConsoleChannel");
	assert (config->getString("logging.loggers.app.name") == "Application");
	assert (config->getString("logging.loggers.app.channel") == "c1");
	assert (config->getString("logging.formatters.f1.class") == "PatternFormatter");
	assert (config->getString("logging.formatters.f1.pattern") == "[%p] %t");

	config->setString("logging.loggers.app.channel", "c2");
	assert (config->getString("logging.loggers.app.channel") == "c2");
	
	AbstractConfiguration::Keys keys;
	config->keys(keys);
	assert (keys.size() == 1);
	assert (std::find(keys.begin(), keys.end(), "logging") != keys.end());

	config->keys("logging", keys);
	assert (keys.size() == 3);
	assert (std::find(keys.begin(), keys.end(), "loggers") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "formatters") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "channels") != keys.end());

	config->keys("logging.formatters", keys);
	assert (keys.size() == 1);
	assert (std::find(keys.begin(), keys.end(), "f1") != keys.end());

	config->keys("logging.formatters.f1", keys);
	assert (keys.size() == 2);
	assert (std::find(keys.begin(), keys.end(), "class") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "pattern") != keys.end());
	
	assert (config->hasProperty("logging.loggers.root.channel.class"));
	config->clear();
	assert (!config->hasProperty("logging.loggers.root.channel.class"));
}


AbstractConfiguration* FilesystemConfigurationTest::allocConfiguration() const
{
	return new FilesystemConfiguration(_path.toString());
}


void FilesystemConfigurationTest::setUp()
{
	Poco::File dir(_path);
	if (dir.exists()) {
		dir.remove(true);
	}
}


void FilesystemConfigurationTest::tearDown()
{
	Poco::File dir(_path);
	if (dir.exists()) {
		dir.remove(true);
	}
}


CppUnit::Test* FilesystemConfigurationTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("FilesystemConfigurationTest");

	AbstractConfigurationTest_addTests(pSuite, FilesystemConfigurationTest);
	CppUnit_addTest(pSuite, FilesystemConfigurationTest, testFilesystemConfiguration);

	return pSuite;
}
