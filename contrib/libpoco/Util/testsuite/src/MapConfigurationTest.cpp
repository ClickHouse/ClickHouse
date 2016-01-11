//
// MapConfigurationTest.cpp
//
// $Id: //poco/1.4/Util/testsuite/src/MapConfigurationTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "MapConfigurationTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Util/MapConfiguration.h"
#include "Poco/AutoPtr.h"


using Poco::Util::AbstractConfiguration;
using Poco::Util::MapConfiguration;
using Poco::AutoPtr;


MapConfigurationTest::MapConfigurationTest(const std::string& name): AbstractConfigurationTest(name)
{
}


MapConfigurationTest::~MapConfigurationTest()
{
}


void MapConfigurationTest::testClear()
{
	AutoPtr<MapConfiguration> pConf = new MapConfiguration;
	
	pConf->setString("foo", "bar");
	assert (pConf->hasProperty("foo"));
	
	pConf->clear();
	assert (!pConf->hasProperty("foo"));
}


AbstractConfiguration* MapConfigurationTest::allocConfiguration() const
{
	return new MapConfiguration;
}


void MapConfigurationTest::setUp()
{
}


void MapConfigurationTest::tearDown()
{
}


CppUnit::Test* MapConfigurationTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("MapConfigurationTest");

	AbstractConfigurationTest_addTests(pSuite, MapConfigurationTest);
	CppUnit_addTest(pSuite, MapConfigurationTest, testClear);

	return pSuite;
}
