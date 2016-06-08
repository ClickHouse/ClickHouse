//
// FilesystemConfigurationTest.h
//
// $Id: //poco/1.4/Util/testsuite/src/FilesystemConfigurationTest.h#1 $
//
// Definition of the FilesystemConfigurationTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef FilesystemConfigurationTest_INCLUDED
#define FilesystemConfigurationTest_INCLUDED


#include "AbstractConfigurationTest.h"
#include "Poco/Util/Util.h"
#include "Poco/Path.h"


class FilesystemConfigurationTest: public AbstractConfigurationTest
{
public:
	FilesystemConfigurationTest(const std::string& name);
	virtual ~FilesystemConfigurationTest();

	void testFilesystemConfiguration();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
	virtual Poco::Util::AbstractConfiguration* allocConfiguration() const;

	Poco::Path const _path;
};


#endif // FilesystemConfigurationTest_INCLUDED
