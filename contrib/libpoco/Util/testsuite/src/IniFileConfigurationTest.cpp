//
// IniFileConfigurationTest.cpp
//
// $Id: //poco/1.4/Util/testsuite/src/IniFileConfigurationTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "IniFileConfigurationTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Util/IniFileConfiguration.h"
#include "Poco/AutoPtr.h"
#include "Poco/Exception.h"
#include <sstream>
#include <algorithm>


using Poco::Util::IniFileConfiguration;
using Poco::Util::AbstractConfiguration;
using Poco::AutoPtr;
using Poco::NotImplementedException;
using Poco::NotFoundException;


IniFileConfigurationTest::IniFileConfigurationTest(const std::string& name): AbstractConfigurationTest(name)
{
}


IniFileConfigurationTest::~IniFileConfigurationTest()
{
}


void IniFileConfigurationTest::testLoad()
{
	static const std::string iniFile = 
		"; comment\n"
		"  ; comment  \n"
		"prop1=value1\n"
		"  prop2 = value2  \n"
		"[section1]\n"
		"prop1 = value3\r\n"
		"\tprop2=value4\r\n"
		";prop3=value7\r\n"
		"\n"
		"  [ section 2 ]\n"
		"prop1 = value 5\n"
		"\t   \n"
		"Prop2 = value6";
		
	std::istringstream istr(iniFile);	
	AutoPtr<IniFileConfiguration> pConf = new IniFileConfiguration(istr);
	
	assert (pConf->getString("prop1") == "value1");
	assert (pConf->getString("prop2") == "value2");
	assert (pConf->getString("section1.prop1") == "value3");
	assert (pConf->getString("Section1.Prop2") == "value4");
	assert (pConf->getString("section 2.prop1") == "value 5");
	assert (pConf->getString("section 2.prop2") == "value6");
	assert (pConf->getString("SECTION 2.PROP2") == "value6");
	
	AbstractConfiguration::Keys keys;
	pConf->keys(keys);
	assert (keys.size() == 4);
	assert (std::find(keys.begin(), keys.end(), "prop1") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "prop2") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "section1") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "section 2") != keys.end());
	
	pConf->keys("Section1", keys);
	assert (keys.size() == 2);
	assert (std::find(keys.begin(), keys.end(), "prop1") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "prop2") != keys.end());
	
	pConf->setString("prop1", "value11");
	assert (pConf->getString("PROP1") == "value11");
	pConf->setString("Prop1", "value12");
	assert (pConf->getString("prop1") == "value12");
	pConf->keys(keys);
	assert (keys.size() == 4);
	assert (std::find(keys.begin(), keys.end(), "prop1") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "prop2") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "section1") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "section 2") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "Prop1") == keys.end());
}


void IniFileConfigurationTest::testCaseInsensitiveRemove()
{
	AutoPtr<AbstractConfiguration> pConf = createConfiguration();
	AbstractConfiguration::Keys keys;

	assert (pConf->hasProperty("Prop1"));
	assert (pConf->hasProperty("prop4.BOOL1"));
	assert (pConf->hasProperty("Prop4.bool2"));
	assert (pConf->hasProperty("PROP4.Bool3"));
	pConf->keys(keys);
	assert (keys.size() == 13);
	pConf->keys("prop4", keys);
	assert (keys.size() == 17);

	pConf->remove("PROP4.Bool1");
	assert (pConf->hasProperty("Prop1"));
	assert (!pConf->hasProperty("prop4.BOOL1"));
	assert (pConf->hasProperty("Prop4.bool2"));
	assert (pConf->hasProperty("PROP4.Bool3"));
	pConf->keys(keys);
	assert (keys.size() == 13);
	pConf->keys("PROP4", keys);
	assert (keys.size() == 16);

	pConf->remove("Prop4");
	assert (pConf->hasProperty("Prop1"));
	assert (!pConf->hasProperty("prop4.BOOL1"));
	assert (!pConf->hasProperty("Prop4.bool2"));
	assert (!pConf->hasProperty("PROP4.Bool3"));
	pConf->keys(keys);
	assert (keys.size() == 12);
	pConf->keys("prop4", keys);
	assert (keys.size() == 0);
}


AbstractConfiguration* IniFileConfigurationTest::allocConfiguration() const
{
	return new IniFileConfiguration;
}


void IniFileConfigurationTest::setUp()
{
}


void IniFileConfigurationTest::tearDown()
{
}


CppUnit::Test* IniFileConfigurationTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("IniFileConfigurationTest");

	AbstractConfigurationTest_addTests(pSuite, IniFileConfigurationTest);
	CppUnit_addTest(pSuite, IniFileConfigurationTest, testLoad);
	CppUnit_addTest(pSuite, IniFileConfigurationTest, testCaseInsensitiveRemove);

	return pSuite;
}
