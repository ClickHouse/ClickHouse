//
// WinRegistryTest.cpp
//
// $Id: //poco/1.4/Util/testsuite/src/WinRegistryTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "WinRegistryTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Util/WinRegistryKey.h"
#include "Poco/Environment.h"
#include "Poco/Exception.h"
#undef min
#undef max
#include <limits>

#if defined(POCO_HAVE_INT64)
using Poco::Int64;
#endif

using Poco::Util::WinRegistryKey;
using Poco::Environment;



WinRegistryTest::WinRegistryTest(const std::string& name): CppUnit::TestCase(name)
{
}


WinRegistryTest::~WinRegistryTest()
{
}


void WinRegistryTest::testRegistry()
{
	WinRegistryKey regKey("HKEY_CURRENT_USER\\Software\\Applied Informatics\\Test");
	if (regKey.exists()) regKey.deleteKey();
	assert (!regKey.exists());

	regKey.setString("name1", "value1");
	assert (regKey.getString("name1") == "value1");
	regKey.setString("name1", "Value1");
	assert (regKey.getString("name1") == "Value1");
	regKey.setString("name2", "value2");
	assert (regKey.getString("name2") == "value2");
	assert (regKey.exists("name1"));
	assert (regKey.exists("name2"));
	assert (regKey.exists());
	
	WinRegistryKey regKeyRO("HKEY_CURRENT_USER\\Software\\Applied Informatics\\Test", true);
	assert (regKeyRO.getString("name1") == "Value1");
	try
	{
		regKeyRO.setString("name1", "newValue1");
	}
	catch (Poco::Exception& exc)
	{
		std::string msg = exc.displayText();
	}
	assert (regKey.getString("name1") == "Value1");
	
	WinRegistryKey::Values vals;
	regKey.values(vals);
	assert (vals.size() == 2);
	assert (vals[0] == "name1" || vals[0] == "name2");
	assert (vals[1] == "name1" || vals[1] == "name2");
	assert (vals[0] != vals[1]);

	Environment::set("FOO", "bar");
	regKey.setStringExpand("name3", "%FOO%");
	assert (regKey.getStringExpand("name3") == "bar");
	
	regKey.setInt("name4", 42);
	assert (regKey.getInt("name4") == 42);
	
	assert (regKey.exists("name4"));
	regKey.deleteValue("name4");
	assert (!regKey.exists("name4"));
	
#if defined(POCO_HAVE_INT64)
	regKey.setInt64("name5", std::numeric_limits<Int64>::max());
	assert (regKey.getInt64("name5") == std::numeric_limits<Int64>::max());

	assert (regKey.exists("name5"));
	regKey.deleteValue("name5");
	assert (!regKey.exists("name5"));
#endif

	const int dataSize = 127;
	std::vector<char> data(dataSize);
	for (int i = 0; i < dataSize; ++i)
		data[i] = rand() % 256;
	regKey.setBinary("binary", data);
	assert (regKey.getBinary("binary") == data);

	assert (regKey.exists("binary"));
	regKey.deleteValue("binary");
	assert (!regKey.exists("binary"));

	regKey.deleteKey();
	assert (!regKey.exists());
}


void WinRegistryTest::setUp()
{
}


void WinRegistryTest::tearDown()
{
}


CppUnit::Test* WinRegistryTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("WinRegistryTest");

	CppUnit_addTest(pSuite, WinRegistryTest, testRegistry);

	return pSuite;
}
