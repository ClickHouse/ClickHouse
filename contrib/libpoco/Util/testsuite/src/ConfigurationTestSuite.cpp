//
// ConfigurationTestSuite.cpp
//
// $Id: //poco/1.4/Util/testsuite/src/ConfigurationTestSuite.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "ConfigurationTestSuite.h"
#include "AbstractConfigurationTest.h"
#include "ConfigurationViewTest.h"
#include "ConfigurationMapperTest.h"
#include "MapConfigurationTest.h"
#include "LayeredConfigurationTest.h"
#include "SystemConfigurationTest.h"
#include "IniFileConfigurationTest.h"
#include "PropertyFileConfigurationTest.h"
#include "XMLConfigurationTest.h"
#include "FilesystemConfigurationTest.h"
#include "LoggingConfiguratorTest.h"
#include "JSONConfigurationTest.h"


CppUnit::Test* ConfigurationTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("ConfigurationTestSuite");

	pSuite->addTest(ConfigurationViewTest::suite());
	pSuite->addTest(ConfigurationMapperTest::suite());
	pSuite->addTest(MapConfigurationTest::suite());
	pSuite->addTest(LayeredConfigurationTest::suite());
	pSuite->addTest(SystemConfigurationTest::suite());
	pSuite->addTest(IniFileConfigurationTest::suite());
	pSuite->addTest(PropertyFileConfigurationTest::suite());
	pSuite->addTest(XMLConfigurationTest::suite());
	pSuite->addTest(FilesystemConfigurationTest::suite());
	pSuite->addTest(LoggingConfiguratorTest::suite());
	pSuite->addTest(JSONConfigurationTest::suite());

	return pSuite;
}
