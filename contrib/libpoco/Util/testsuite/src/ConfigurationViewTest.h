//
// ConfigurationViewTest.h
//
// $Id: //poco/1.4/Util/testsuite/src/ConfigurationViewTest.h#1 $
//
// Definition of the ConfigurationViewTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef ConfigurationViewTest_INCLUDED
#define ConfigurationViewTest_INCLUDED


#include "AbstractConfigurationTest.h"
#include "Poco/Util/Util.h"


class ConfigurationViewTest: public AbstractConfigurationTest
{
public:
	ConfigurationViewTest(const std::string& name);
	virtual ~ConfigurationViewTest();

	void testView();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
	virtual Poco::Util::AbstractConfiguration* allocConfiguration() const;
};


#endif // ConfigurationViewTest_INCLUDED
