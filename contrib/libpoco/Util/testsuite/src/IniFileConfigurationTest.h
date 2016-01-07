//
// IniFileConfigurationTest.h
//
// $Id: //poco/1.4/Util/testsuite/src/IniFileConfigurationTest.h#1 $
//
// Definition of the IniFileConfigurationTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef IniFileConfigurationTest_INCLUDED
#define IniFileConfigurationTest_INCLUDED


#include "AbstractConfigurationTest.h"
#include "Poco/Util/Util.h"


class IniFileConfigurationTest: public AbstractConfigurationTest
{
public:
	IniFileConfigurationTest(const std::string& name);
	virtual ~IniFileConfigurationTest();

	void testLoad();
	void testCaseInsensitiveRemove();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
	virtual Poco::Util::AbstractConfiguration* allocConfiguration() const;
};


#endif // IniFileConfigurationTest_INCLUDED
