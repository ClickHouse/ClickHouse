//
// LayeredConfigurationTest.h
//
// $Id: //poco/1.4/Util/testsuite/src/LayeredConfigurationTest.h#1 $
//
// Definition of the LayeredConfigurationTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef LayeredConfigurationTest_INCLUDED
#define LayeredConfigurationTest_INCLUDED


#include "AbstractConfigurationTest.h"
#include "Poco/Util/Util.h"


class LayeredConfigurationTest: public AbstractConfigurationTest
{
public:
	LayeredConfigurationTest(const std::string& name);
	virtual ~LayeredConfigurationTest();

	void testEmpty();
	void testOneLayer();
	void testTwoLayers();
	void testThreeLayers();
	void testRemove();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
	virtual Poco::Util::AbstractConfiguration* allocConfiguration() const;
};


#endif // LayeredConfigurationTest_INCLUDED
