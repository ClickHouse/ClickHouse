//
// PropertyFileConfigurationTest.h
//
// $Id: //poco/1.4/Util/testsuite/src/PropertyFileConfigurationTest.h#1 $
//
// Definition of the PropertyFileConfigurationTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef PropertyFileConfigurationTest_INCLUDED
#define PropertyFileConfigurationTest_INCLUDED


#include "AbstractConfigurationTest.h"
#include "Poco/Util/Util.h"


class PropertyFileConfigurationTest: public AbstractConfigurationTest
{
public:
	PropertyFileConfigurationTest(const std::string& name);
	virtual ~PropertyFileConfigurationTest();

	void testLoad();
	void testSave();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
	virtual Poco::Util::AbstractConfiguration* allocConfiguration() const;
};


#endif // PropertyFileConfigurationTest_INCLUDED
