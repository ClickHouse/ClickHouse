//
// AbstractConfigurationTest.h
//
// $Id: //poco/1.4/Util/testsuite/src/AbstractConfigurationTest.h#1 $
//
// Definition of the AbstractConfigurationTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef AbstractConfigurationTest_INCLUDED
#define AbstractConfigurationTest_INCLUDED


#include "Poco/Util/Util.h"
#include "CppUnit/TestCase.h"
#include "Poco/AutoPtr.h"
#include "Poco/Util/AbstractConfiguration.h"


class AbstractConfigurationTest: public CppUnit::TestCase
{
public:
	AbstractConfigurationTest(const std::string& name);
	virtual ~AbstractConfigurationTest();

	void testHasProperty();
	void testGetString();
	void testGetInt();
	void testGetInt64();
	void testGetDouble();
	void testGetBool();
	void testExpand();
	void testSetString();
	void testSetInt();
	void testSetUInt();
	void testSetInt64();
	void testSetUInt64();
	void testSetDouble();
	void testSetBool();
	void testKeys();
	void testRemove();
	void testChangeEvents();
	void testRemoveEvents();
	
	void setUp();
	void tearDown();
	
	void onPropertyChanging(const void*, Poco::Util::AbstractConfiguration::KeyValue& kv);
	void onPropertyChanged(const void*, const Poco::Util::AbstractConfiguration::KeyValue& kv);
	void onPropertyRemoving(const void*, const std::string& key);
	void onPropertyRemoved(const void*, const std::string& key);

protected:
	virtual Poco::Util::AbstractConfiguration* allocConfiguration() const = 0;
	virtual Poco::AutoPtr<Poco::Util::AbstractConfiguration> createConfiguration() const;

	std::string _changingKey;
	std::string _changingValue;
	std::string _changedKey;
	std::string _changedValue;
	std::string _removingKey;
	std::string _removedKey;
};


#define AbstractConfigurationTest_addTests(suite, cls) \
	do { \
		CppUnit_addTest(suite, cls, testHasProperty); \
		CppUnit_addTest(suite, cls, testGetString); \
		CppUnit_addTest(suite, cls, testGetInt); \
		CppUnit_addTest(suite, cls, testGetInt64); \
		CppUnit_addTest(suite, cls, testGetDouble); \
		CppUnit_addTest(suite, cls, testGetBool); \
		CppUnit_addTest(suite, cls, testExpand); \
		CppUnit_addTest(suite, cls, testSetString); \
		CppUnit_addTest(suite, cls, testSetInt); \
		CppUnit_addTest(suite, cls, testSetInt64); \
		CppUnit_addTest(suite, cls, testSetDouble); \
		CppUnit_addTest(suite, cls, testSetBool); \
		CppUnit_addTest(suite, cls, testKeys); \
		CppUnit_addTest(suite, cls, testRemove); \
		CppUnit_addTest(suite, cls, testChangeEvents); \
		CppUnit_addTest(suite, cls, testRemoveEvents); \
	} while(0)


#endif // AbstractConfigurationTest_INCLUDED
