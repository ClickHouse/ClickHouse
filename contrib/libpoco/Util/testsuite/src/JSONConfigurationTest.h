//
// JSONConfigurationTest.h
//
// $Id$
//
// Definition of the JSONConfigurationTest class.
//
// Copyright (c) 2004-2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef JSONConfigurationTest_INCLUDED
#define JSONConfigurationTest_INCLUDED


#include "AbstractConfigurationTest.h"
#include "Poco/Util/Util.h"


class JSONConfigurationTest: public AbstractConfigurationTest
{
public:
	JSONConfigurationTest(const std::string& name);
	virtual ~JSONConfigurationTest();

	void testLoad();
	void testSetArrayElement();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
	virtual Poco::Util::AbstractConfiguration* allocConfiguration() const;
};


#endif // JSONConfigurationTest_INCLUDED
