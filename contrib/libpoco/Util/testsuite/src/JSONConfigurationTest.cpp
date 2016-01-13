//
// JSONConfigurationTest.cpp
//
// $Id$
//
// Copyright (c) 2004-2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "JSONConfigurationTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Util/JSONConfiguration.h"
#include "Poco/JSON/JSONException.h"


using Poco::Util::JSONConfiguration;
using Poco::Util::AbstractConfiguration;
using Poco::AutoPtr;
using Poco::NotImplementedException;
using Poco::NotFoundException;
using Poco::JSON::JSONException;


JSONConfigurationTest::JSONConfigurationTest(const std::string& name) : AbstractConfigurationTest(name)
{
}


JSONConfigurationTest::~JSONConfigurationTest()
{
}


void JSONConfigurationTest::testLoad()
{
	JSONConfiguration config;

	std::string json = "{ \"config\" : "
							" { \"prop1\" : \"value1\", "
							" \"prop2\" : 10, "
							" \"prop3\" : [ \"element1\", \"element2\" ], "
							" \"prop4\" : { \"prop5\" : false, "
											" \"prop6\" : null } "
							" }"
						"}";

	std::istringstream iss(json);
	try
	{
		config.load(iss);
	}
	catch(JSONException jsone)
	{
		std::cout << jsone.message() << std::endl;
		assert(false);
	}

	std::string property1 = config.getString("config.prop1");
	assert(property1.compare("value1") == 0);

	int property2 = config.getInt("config.prop2");
	assert(property2 == 10);

	int nonExistingProperty = config.getInt("config.prop7", 5);
	assert(nonExistingProperty == 5);

	std::string arrProperty = config.getString("config.prop3[1]");
	assert(arrProperty.compare("element2") == 0);

	bool property35 = config.getBool("config.prop4.prop5");
	assert(! property35);

	try
	{
		config.getString("propertyUnknown");
		assert(true);
	}
	catch(NotFoundException nfe)
	{
	}

}


void JSONConfigurationTest::testSetArrayElement()
{
	JSONConfiguration config;

	std::string json = "{ \"config\" : "
							" { \"prop1\" : \"value1\", "
							" \"prop2\" : 10, "
							" \"prop3\" : [ \"element1\", \"element2\" ], "
							" \"prop4\" : { \"prop5\" : false, "
											" \"prop6\" : null } "
							" }"
						"}";

	std::istringstream iss(json);
	config.load(iss);

	// config.prop3[0] = "foo"
	config.setString("config.prop3[0]", "foo");
	assert(config.getString("config.prop3[0]") == "foo");

	// config.prop3[1] = "bar"
	config.setString("config.prop3[1]", "bar");
	assert(config.getString("config.prop3[1]") == "bar");

	// config.prop3[3] = "baz"
	config.setString("config.prop3[3]", "baz");
	assert(config.getString("config.prop3[3]") == "baz");
}


AbstractConfiguration* JSONConfigurationTest::allocConfiguration() const
{
	return new JSONConfiguration;
}


void JSONConfigurationTest::setUp()
{
}


void JSONConfigurationTest::tearDown()
{
}


CppUnit::Test* JSONConfigurationTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("JSONConfigurationTest");

	AbstractConfigurationTest_addTests(pSuite, JSONConfigurationTest);
	CppUnit_addTest(pSuite, JSONConfigurationTest, testLoad);
	CppUnit_addTest(pSuite, JSONConfigurationTest, testSetArrayElement);

	return pSuite;
}
