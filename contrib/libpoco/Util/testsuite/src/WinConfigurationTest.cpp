//
// WinConfigurationTest.cpp
//
// $Id: //poco/1.4/Util/testsuite/src/WinConfigurationTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "WinConfigurationTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Util/WinRegistryConfiguration.h"
#include "Poco/Util/WinRegistryKey.h"
#include "Poco/Environment.h"
#include "Poco/AutoPtr.h"
#include "Poco/types.h"
#undef min
#undef max
#include <limits>


using Poco::Util::WinRegistryConfiguration;
using Poco::Util::WinRegistryKey;
using Poco::Environment;
using Poco::AutoPtr;
using Poco::Int64;
using Poco::UInt64;


WinConfigurationTest::WinConfigurationTest(const std::string& name): CppUnit::TestCase(name)
{
}


WinConfigurationTest::~WinConfigurationTest()
{
}


void WinConfigurationTest::testConfiguration()
{
	WinRegistryKey regKey("HKEY_CURRENT_USER\\Software\\Applied Informatics\\Test");
	if (regKey.exists()) regKey.deleteKey();
	assert (!regKey.exists());

	AutoPtr<WinRegistryConfiguration> pReg = new WinRegistryConfiguration("HKEY_CURRENT_USER\\Software\\Applied Informatics\\Test");
	pReg->setString("name1", "value1");
	assert (pReg->getString("name1") == "value1");
	pReg->setInt("name1", 1); // overwrite should also change type
	assert (pReg->getInt("name1") == 1);
	pReg->setString("name2", "value2");
	assert (pReg->getString("name2") == "value2");
#if defined(POCO_HAVE_INT64)
	pReg->setUInt64("name2", std::numeric_limits<UInt64>::max()); // overwrite should also change type
	assert (pReg->getUInt64("name2") == std::numeric_limits<UInt64>::max());
	pReg->setInt64("name2", std::numeric_limits<Int64>::min()); 
	assert (pReg->getInt64("name2") == std::numeric_limits<Int64>::min());

	/// write real int64 value type
	regKey.setInt64("name3", std::numeric_limits<Int64>::max());
	assert (pReg->getInt64("name3") == std::numeric_limits<Int64>::max());
#endif

	/// create fake binary data
	const int dataSize = 127;
	std::vector<char> data(dataSize);
	for (int i = 0; i < dataSize; ++i)
		data[i] = rand() % 256;

	regKey.setBinary("name4", data);
	assert (pReg->getString("name4") == std::string(data.begin(), data.end()));


	assert (pReg->hasProperty("name1"));
	assert (pReg->hasProperty("name2"));
	
	std::string dfl = pReg->getString("nonexistent", "default");
	assert (dfl == "default");
	
	AutoPtr<Poco::Util::AbstractConfiguration> pView = pReg->createView("config");
	dfl = pView->getString("sub.foo", "default");
	assert (dfl == "default");
	
	pView->setString("sub.foo", "bar");
	assert (pView->getString("sub.foo", "default") == "bar");

	std::string value;
	assert (pReg->convertToRegFormat("A.B.C", value) == "A\\B");
	assert (value == "C");

	Poco::Util::AbstractConfiguration::Keys keys;
	pReg->keys(keys);
	assert (keys.size() == 5);
	assert (std::find(keys.begin(), keys.end(), "name1") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "name2") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "name3") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "name4") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "config") != keys.end());

	pReg->keys("config", keys);
	assert (keys.size() == 1);
	assert (std::find(keys.begin(), keys.end(), "sub") != keys.end());

	AutoPtr<WinRegistryConfiguration> pRootReg = new WinRegistryConfiguration("");

	assert (pRootReg->getInt("HKEY_CURRENT_USER.Software.Applied Informatics.Test.name1") == 1);

	pRootReg->keys(keys);
#if defined(_WIN32_WCE)
	assert (keys.size() == 4);
	assert (std::find(keys.begin(), keys.end(), "HKEY_CLASSES_ROOT") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "HKEY_CURRENT_USER") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "HKEY_LOCAL_MACHINE") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "HKEY_USERS") != keys.end());
#else
	assert (keys.size() == 6);
	assert (std::find(keys.begin(), keys.end(), "HKEY_CLASSES_ROOT") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "HKEY_CURRENT_CONFIG") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "HKEY_CURRENT_USER") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "HKEY_LOCAL_MACHINE") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "HKEY_PERFORMANCE_DATA") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "HKEY_USERS") != keys.end());
#endif

	pRootReg->keys("HKEY_CURRENT_USER.Software.Applied Informatics.Test", keys);
	assert (keys.size() == 5);
	assert (std::find(keys.begin(), keys.end(), "name1") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "name2") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "name3") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "name4") != keys.end());
	assert (std::find(keys.begin(), keys.end(), "config") != keys.end());
}


void WinConfigurationTest::setUp()
{
}


void WinConfigurationTest::tearDown()
{
}


CppUnit::Test* WinConfigurationTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("WinConfigurationTest");

	CppUnit_addTest(pSuite, WinConfigurationTest, testConfiguration);

	return pSuite;
}
