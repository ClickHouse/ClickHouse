//
// MapConfigurationTest.h
//
// $Id: //poco/1.4/Util/testsuite/src/MapConfigurationTest.h#1 $
//
// Definition of the MapConfigurationTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MapConfigurationTest_INCLUDED
#define MapConfigurationTest_INCLUDED


#include "AbstractConfigurationTest.h"
#include "Poco/Util/Util.h"


class MapConfigurationTest: public AbstractConfigurationTest
{
public:
	MapConfigurationTest(const std::string& name);
	virtual ~MapConfigurationTest();

	void testClear();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
	virtual Poco::Util::AbstractConfiguration* allocConfiguration() const;
};


#endif // MapConfigurationTest_INCLUDED
