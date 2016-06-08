//
// ConfigurationViewTest.cpp
//
// $Id: //poco/1.4/Util/testsuite/src/ConfigurationViewTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "ConfigurationViewTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Util/MapConfiguration.h"
#include "Poco/AutoPtr.h"
#include "Poco/Exception.h"
#include <algorithm>


using Poco::Util::AbstractConfiguration;
using Poco::Util::MapConfiguration;
using Poco::AutoPtr;


ConfigurationViewTest::ConfigurationViewTest(const std::string& name): AbstractConfigurationTest(name)
{
}


ConfigurationViewTest::~ConfigurationViewTest()
{
}


void ConfigurationViewTest::testView()
{
	AutoPtr<AbstractConfiguration> pConf = createConfiguration();
	AutoPtr<AbstractConfiguration> pView = pConf->createView("");
	assert (pView->hasProperty("prop1"));
	assert (pView->hasProperty("prop2"));

	AbstractConfiguration::Keys keys;
	pView->keys(keys);
	assert (keys.size() == 13);
	assert (std::find(keys.begin(), keys.end(), "prop1") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "prop2") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "prop3") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "prop4") != keys.end());
	
	assert (pView->getString("prop1") == "foo");
	assert (pView->getString("prop3.string1") == "foo");
	
	pView->setString("prop6", "foobar");
	assert (pConf->getString("prop6") == "foobar");
	
	pView = pConf->createView("prop1");
	pView->keys(keys);
	assert (keys.empty());
	assert (pView->hasProperty("prop1"));

	pView->setString("prop11", "foobar");
	assert (pConf->getString("prop1.prop11") == "foobar");

	pView = pConf->createView("prop3");
	pView->keys(keys);
	assert (keys.size() == 2);
	assert (std::find(keys.begin(), keys.end(), "string1") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "string2") != keys.end());
	
	assert (pView->getString("string1") == "foo");
	assert (pView->getString("string2") == "bar");

	pView->setString("string3", "foobar");
	assert (pConf->getString("prop3.string3") == "foobar");

	pView = pConf->createView("prop5");
	pView->keys(keys);
	assert (keys.size() == 4);
	assert (std::find(keys.begin(), keys.end(), "string1") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "string1") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "sub1") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "sub2") != keys.end());
	
	assert (pView->getString("sub1.string1") == "FOO");
	assert (pView->getString("sub2.string2") == "Bar");

	pView = pConf->createView("prop5.sub1");
	pView->keys(keys);
	assert (keys.size() == 2);
	assert (std::find(keys.begin(), keys.end(), "string1") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "string2") != keys.end());
	
	assert (pView->getString("string1") == "FOO");
	assert (pView->getString("string2") == "BAR");
	
	pView->setString("string3", "foobar");
	assert (pConf->getString("prop5.sub1.string3") == "foobar");

	pView->remove("string3");
	assert (!pConf->hasProperty("prop5.sub1.string3"));
}


AbstractConfiguration* ConfigurationViewTest::allocConfiguration() const
{
	return new MapConfiguration;
}


void ConfigurationViewTest::setUp()
{
}


void ConfigurationViewTest::tearDown()
{
}


CppUnit::Test* ConfigurationViewTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("ConfigurationViewTest");

	AbstractConfigurationTest_addTests(pSuite, ConfigurationViewTest);
	CppUnit_addTest(pSuite, ConfigurationViewTest, testView);

	return pSuite;
}
