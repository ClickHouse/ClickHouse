//
// AbstractConfigurationTest.cpp
//
// $Id: //poco/1.4/Util/testsuite/src/AbstractConfigurationTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "AbstractConfigurationTest.h"
#include "CppUnit/TestCaller.h"
#include "Poco/Util/MapConfiguration.h"
#include "Poco/AutoPtr.h"
#include "Poco/Exception.h"
#include "Poco/Delegate.h"
#include "Poco/NumberFormatter.h"
#include "Poco/Types.h"
#include <algorithm>
#undef min
#undef max
#include <limits>


using Poco::Util::AbstractConfiguration;
using Poco::Util::MapConfiguration;
using Poco::NumberFormatter;
using Poco::AutoPtr;
using Poco::Int64;
using Poco::UInt64;


AbstractConfigurationTest::AbstractConfigurationTest(const std::string& name): CppUnit::TestCase(name)
{
}


AbstractConfigurationTest::~AbstractConfigurationTest()
{
}


void AbstractConfigurationTest::testHasProperty()
{
	AutoPtr<AbstractConfiguration> pConf = createConfiguration();

	assert (pConf->hasProperty("prop1"));
	assert (pConf->hasProperty("prop2"));
	assert (pConf->hasProperty("prop3.string1"));
	assert (!pConf->hasProperty("prop3.string3"));
	assert (!pConf->hasProperty("foobar"));
}


void AbstractConfigurationTest::testGetString()
{
	AutoPtr<AbstractConfiguration> pConf = createConfiguration();
	
	assert (pConf->getString("prop1") == "foo");
	assert (pConf->getString("prop2") == "bar");
	assert (pConf->getString("prop3.string1") == "foo");
	assert (pConf->getString("prop3.string2") == "bar");
	assert (pConf->getString("ref1") == "foobar");
	assert (pConf->getRawString("ref1") == "${prop3.string1}${prop3.string2}");
	
	try
	{
		std::string res = pConf->getString("foo");
		fail("nonexistent property - must throw");
	}
	catch (Poco::NotFoundException&)
	{
	}
	
	assert (pConf->getString("prop1", "FOO") == "foo");
	assert (pConf->getString("prop2", "BAR") == "bar");
	assert (pConf->getString("prop3.string1", "FOO") == "foo");
	assert (pConf->getString("prop3.string2", "BAR") == "bar");
	assert (pConf->getString("prop3.string3", "FOOBAR") == "FOOBAR");
}


void AbstractConfigurationTest::testGetInt()
{
	AutoPtr<AbstractConfiguration> pConf = createConfiguration();

	assert (pConf->getInt("prop4.int1") == 42);
	assert (pConf->getInt("prop4.int2") == -42);
	assert (pConf->getInt("prop4.hex") == 0x1f);
	assert (pConf->getInt("ref2") == 42);
	
	try
	{
		pConf->getInt("prop1");
		fail("not a number - must throw");
	}
	catch (Poco::SyntaxException&)
	{
	}
	
	assert (pConf->getInt("prop4.int1", 100) == 42);
	assert (pConf->getInt("prop4.int2", 100) == -42);
	assert (pConf->getInt("prop4.int3", 100) == 100);
}


void AbstractConfigurationTest::testGetInt64()
{
#if defined(POCO_HAVE_INT64)
	AutoPtr<AbstractConfiguration> pConf = createConfiguration();

	assert (pConf->getInt64("prop4.bigint1") == std::numeric_limits<Int64>::max());
	assert (pConf->getInt64("prop4.bigint2") == std::numeric_limits<Int64>::min());
	assert (pConf->getUInt64("prop4.biguint") == std::numeric_limits<UInt64>::max());
	assert (pConf->getInt64("ref2") == 42);

	try
	{
		Int64 x = pConf->getInt64("prop1");
		x=x;
		fail("not a number - must throw");
	}
	catch (Poco::SyntaxException&)
	{
	}

	assert (pConf->getInt64("prop4.bigint1", 100) == std::numeric_limits<Int64>::max());
	assert (pConf->getInt64("prop4.bigint2", 100) == std::numeric_limits<Int64>::min());
	assert (pConf->getUInt64("prop4.biguint", 100) == std::numeric_limits<UInt64>::max());
#endif
}


void AbstractConfigurationTest::testGetDouble()
{
	AutoPtr<AbstractConfiguration> pConf = createConfiguration();

	assert (pConf->getDouble("prop4.double1") == 1);
	assert (pConf->getDouble("prop4.double2") == -1.5);
	
	try
	{
		pConf->getDouble("prop1");
		fail("not a number - must throw");
	}
	catch (Poco::SyntaxException&)
	{
	}
	
	assert (pConf->getDouble("prop4.double1", 123.5) == 1);
	assert (pConf->getDouble("prop4.double2", 123.5) == -1.5);
	assert (pConf->getDouble("prop4.double3", 123.5) == 123.5);
}


void AbstractConfigurationTest::testGetBool()
{
	AutoPtr<AbstractConfiguration> pConf = createConfiguration();

	assert (pConf->getBool("prop4.bool1"));
	assert (!pConf->getBool("prop4.bool2"));
	assert (pConf->getBool("prop4.bool3"));
	assert (!pConf->getBool("prop4.bool4"));
	assert (pConf->getBool("prop4.bool5"));
	assert (!pConf->getBool("prop4.bool6"));
	assert (pConf->getBool("prop4.bool7"));
	assert (!pConf->getBool("prop4.bool8"));

	try
	{
		pConf->getBool("prop1");
		fail("not a boolean - must throw");
	}
	catch (Poco::SyntaxException&)
	{
	}

	assert (pConf->getBool("prop4.bool1", false));
	assert (!pConf->getBool("prop4.bool2", true));
	assert (pConf->getBool("prop4.boolx", true));
	assert (!pConf->getBool("prop4.booly", false));
}


void AbstractConfigurationTest::testExpand()
{
	AutoPtr<AbstractConfiguration> pConf = createConfiguration();

	assert (pConf->getString("ref1") == "foobar");
	assert (pConf->getInt("ref2") == 42);
	
	try
	{
		std::string s = pConf->getString("ref3");
		fail("circular reference - must throw");
	}
	catch (Poco::CircularReferenceException&)
	{
	}
	
	assert (pConf->getString("ref5") == "${refx}");
	assert (pConf->getString("ref6") == "${refx}");
	
	assert (pConf->expand("answer=${prop4.int1}") == "answer=42");
	assert (pConf->expand("bool5='${prop4.bool5}'") == "bool5='Yes'");
	assert (pConf->expand("undef='${undef}'") == "undef='${undef}'");
	assert (pConf->expand("deep='${ref1}'") == "deep='foobar'");
	assert (pConf->expand("deep='${ref7}'") == "deep='foobar'");
	
	assert (pConf->getString("dollar.atend") == "foo$");
	assert (pConf->getString("dollar.middle") == "foo$bar");
}


void AbstractConfigurationTest::testSetString()
{
	AutoPtr<AbstractConfiguration> pConf = createConfiguration();

	pConf->setString("set.string1", "foobar");
	pConf->setString("set.string2", "");
	assert (pConf->getString("set.string1") == "foobar");
	assert (pConf->getString("set.string2") == "");
}

void AbstractConfigurationTest::testSetInt()
{
	AutoPtr<AbstractConfiguration> pConf = createConfiguration();

	pConf->setInt("set.int1", 42);
	pConf->setInt("set.int2", -100);
	pConf->setInt("set.uint", 42U);
	assert (pConf->getInt("set.int1") == 42);
	assert (pConf->getInt("set.int2") == -100);
	assert (pConf->getInt("set.uint") == 42U);
}


void AbstractConfigurationTest::testSetInt64()
{
#if defined(POCO_HAVE_INT64)
	AutoPtr<AbstractConfiguration> pConf = createConfiguration();

	pConf->setInt64("set.bigint1", std::numeric_limits<Int64>::max());
	pConf->setInt64("set.bigint2", std::numeric_limits<Int64>::min());
	pConf->setInt64("set.biguint", std::numeric_limits<UInt64>::max());

	assert (pConf->getInt64("set.bigint1") == std::numeric_limits<Int64>::max());
	assert (pConf->getInt64("set.bigint2") == std::numeric_limits<Int64>::min());
	assert (pConf->getInt64("set.biguint") == std::numeric_limits<UInt64>::max());
#endif //defined(POCO_HAVE_INT64)
}


void AbstractConfigurationTest::testSetDouble()
{
	AutoPtr<AbstractConfiguration> pConf = createConfiguration();

	pConf->setDouble("set.double1", 1.5);
	pConf->setDouble("set.double2", -1.5);
	assert (pConf->getDouble("set.double1") == 1.5);
	assert (pConf->getDouble("set.double2") == -1.5);	
}


void AbstractConfigurationTest::testSetBool()
{
	AutoPtr<AbstractConfiguration> pConf = createConfiguration();

	pConf->setBool("set.bool1", true);
	pConf->setBool("set.bool2", false);
	assert (pConf->getBool("set.bool1"));
	assert (!pConf->getBool("set.bool2"));
}


void AbstractConfigurationTest::testChangeEvents()
{
	AutoPtr<AbstractConfiguration> pConf = createConfiguration();

	pConf->propertyChanging += Poco::delegate(this, &AbstractConfigurationTest::onPropertyChanging);
	pConf->propertyChanged += Poco::delegate(this, &AbstractConfigurationTest::onPropertyChanged);
	
	pConf->setString("set.string1", "foobar");
	assert (_changingKey == "set.string1");
	assert (_changingValue == "foobar");
	assert (_changedKey == "set.string1");
	assert (_changedValue == "foobar");
	
	pConf->setInt("set.int1", 42);
	assert (_changingKey == "set.int1");
	assert (_changingValue == "42");
	assert (_changedKey == "set.int1");
	assert (_changedValue == "42");
	
	pConf->setDouble("set.double1", 1.5);
	assert (_changingKey == "set.double1");
	assert (_changingValue == "1.5");
	assert (_changedKey == "set.double1");
	assert (_changedValue == "1.5");
	
	pConf->setBool("set.bool1", true);
	assert (_changingKey == "set.bool1");
	assert (_changingValue == "true");
	assert (_changedKey == "set.bool1");
	assert (_changedValue == "true");
}


void AbstractConfigurationTest::testRemoveEvents()
{
	AutoPtr<AbstractConfiguration> pConf = createConfiguration();

	pConf->propertyRemoving += Poco::delegate(this, &AbstractConfigurationTest::onPropertyRemoving);
	pConf->propertyRemoved += Poco::delegate(this, &AbstractConfigurationTest::onPropertyRemoved);

	pConf->remove("prop4.bool1");
	assert (_removingKey == "prop4.bool1");
	assert (_removedKey == "prop4.bool1");
}


void AbstractConfigurationTest::testKeys()
{
	AutoPtr<AbstractConfiguration> pConf = createConfiguration();

	AbstractConfiguration::Keys keys;
	pConf->keys(keys);
	assert (keys.size() == 13);
	assert (std::find(keys.begin(), keys.end(), "prop1") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "prop2") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "prop3") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "prop4") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "ref1") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "ref2") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "ref3") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "ref4") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "ref5") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "ref6") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "ref7") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "dollar") != keys.end());

	pConf->keys("prop1", keys);
	assert (keys.empty());
	
	pConf->keys("prop3", keys);
	assert (keys.size() == 2);
	assert (std::find(keys.begin(), keys.end(), "string1") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "string2") != keys.end());

	assert (!pConf->hasProperty("nonexistent.sub"));
	pConf->keys("nonexistent.sub", keys);
	assert (keys.empty());
}


void AbstractConfigurationTest::testRemove()
{
	AutoPtr<AbstractConfiguration> pConf = createConfiguration();
	AbstractConfiguration::Keys keys;

	assert (pConf->hasProperty("prop1"));
	assert (pConf->hasProperty("prop4.bool1"));
	assert (pConf->hasProperty("prop4.bool2"));
	assert (pConf->hasProperty("prop4.bool3"));
	pConf->keys(keys);
	assert (keys.size() == 13);
	pConf->keys("prop4", keys);
	assert (keys.size() == 17);

	pConf->remove("prop4.bool1");
	assert (!pConf->hasProperty("prop4.bool1"));
	assert (pConf->hasProperty("prop4.bool2"));
	assert (pConf->hasProperty("prop4.bool3"));
	pConf->keys(keys);
	assert (keys.size() == 13);
	pConf->keys("prop4", keys);
	assert (keys.size() == 16);

	pConf->remove("prop4");
	assert (!pConf->hasProperty("prop4.bool1"));
	assert (!pConf->hasProperty("prop4.bool2"));
	assert (!pConf->hasProperty("prop4.bool3"));
	assert (pConf->hasProperty("prop1"));
	pConf->keys(keys);
	assert (keys.size() == 12);
	pConf->keys("prop4", keys);
	assert (keys.size() == 0);

	assert (!pConf->hasProperty("nonexistent.sub.value"));
	pConf->remove("nonexistent.sub.value");
	assert (!pConf->hasProperty("nonexistent.sub.value"));
}


Poco::AutoPtr<AbstractConfiguration> AbstractConfigurationTest::createConfiguration() const
{
	Poco::AutoPtr<AbstractConfiguration> pConfig = allocConfiguration();
	
	pConfig->setString("prop1", "foo");
	pConfig->setString("prop2", "bar");
	pConfig->setString("prop3.string1", "foo");
	pConfig->setString("prop3.string2", "bar");
	pConfig->setString("prop4.int1", "42");
	pConfig->setString("prop4.int2", "-42");
	pConfig->setString("prop4.uint", NumberFormatter::format(std::numeric_limits<unsigned>::max()));
#if defined(POCO_HAVE_INT64)
	pConfig->setString("prop4.bigint1", NumberFormatter::format(std::numeric_limits<Int64>::max()));
	pConfig->setString("prop4.bigint2", NumberFormatter::format(std::numeric_limits<Int64>::min()));
	pConfig->setString("prop4.biguint", NumberFormatter::format(std::numeric_limits<UInt64>::max()));
#else /// just to make sure property count is consistent
	pConfig->setString("prop4.bigint1", 0));
	pConfig->setString("prop4.bigint2", 0));
	pConfig->setString("prop4.biguint", 0));
#endif
	pConfig->setString("prop4.hex", "0x1f");
	pConfig->setString("prop4.double1", "1");
	pConfig->setString("prop4.double2", "-1.5");
	pConfig->setString("prop4.bool1", "1");
	pConfig->setString("prop4.bool2", "0");
	pConfig->setString("prop4.bool3", "True");
	pConfig->setString("prop4.bool4", "FALSE");
	pConfig->setString("prop4.bool5", "Yes");
	pConfig->setString("prop4.bool6", "no");
	pConfig->setString("prop4.bool7", "ON");
	pConfig->setString("prop4.bool8", "Off");
	pConfig->setString("prop5.string1", "foo");
	pConfig->setString("prop5.string2", "bar");
	pConfig->setString("prop5.sub1.string1", "FOO");
	pConfig->setString("prop5.sub1.string2", "BAR");
	pConfig->setString("prop5.sub2.string1", "Foo");
	pConfig->setString("prop5.sub2.string2", "Bar");
	pConfig->setString("ref1", "${prop3.string1}${prop3.string2}");
	pConfig->setString("ref2", "${prop4.int1}");
	pConfig->setString("ref3", "${ref4}");
	pConfig->setString("ref4", "${ref3}");
	pConfig->setString("ref5", "${refx}");
	pConfig->setString("ref6", "${refx");
	pConfig->setString("ref7", "${ref1}");
	pConfig->setString("dollar.atend", "foo$");
	pConfig->setString("dollar.middle", "foo$bar");

	return pConfig;
}


void AbstractConfigurationTest::setUp()
{
	_changingKey.clear();
	_changingValue.clear();
	_changedKey.clear();
	_changedValue.clear();
	_removingKey.clear();
	_removedKey.clear();
}


void AbstractConfigurationTest::tearDown()
{
}


void AbstractConfigurationTest::onPropertyChanging(const void*, AbstractConfiguration::KeyValue& kv)
{
	_changingKey   = kv.key();
	_changingValue = kv.value();
}


void AbstractConfigurationTest::onPropertyChanged(const void*, const AbstractConfiguration::KeyValue& kv)
{
	_changedKey   = kv.key();
	_changedValue = kv.value();
}


void AbstractConfigurationTest::onPropertyRemoving(const void*, const std::string& key)
{
	_removingKey = key;
}


void AbstractConfigurationTest::onPropertyRemoved(const void*, const std::string& key)
{
	_removedKey = key;
}

