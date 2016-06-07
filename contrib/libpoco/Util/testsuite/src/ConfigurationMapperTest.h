//
// ConfigurationMapperTest.h
//
// $Id: //poco/1.4/Util/testsuite/src/ConfigurationMapperTest.h#1 $
//
// Definition of the ConfigurationMapperTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef ConfigurationMapperTest_INCLUDED
#define ConfigurationMapperTest_INCLUDED


#include "AbstractConfigurationTest.h"
#include "Poco/Util/Util.h"


class ConfigurationMapperTest: public AbstractConfigurationTest
{
public:
	ConfigurationMapperTest(const std::string& name);
	virtual ~ConfigurationMapperTest();

	void testMapper1();
	void testMapper2();
	void testMapper3();
	void testMapper4();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
	virtual Poco::Util::AbstractConfiguration* allocConfiguration() const;
};


#endif // ConfigurationMapperTest_INCLUDED
