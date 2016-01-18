//
// XMLConfigurationTest.h
//
// $Id: //poco/1.4/Util/testsuite/src/XMLConfigurationTest.h#2 $
//
// Definition of the XMLConfigurationTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef XMLConfigurationTest_INCLUDED
#define XMLConfigurationTest_INCLUDED


#include "AbstractConfigurationTest.h"
#include "Poco/Util/Util.h"


class XMLConfigurationTest: public AbstractConfigurationTest
{
public:
	XMLConfigurationTest(const std::string& name);
	virtual ~XMLConfigurationTest();

	void testLoad();
	void testSave();
	void testLoadAppendSave();
	void testOtherDelimiter();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
	virtual Poco::Util::AbstractConfiguration* allocConfiguration() const;
};


#endif // XMLConfigurationTest_INCLUDED
