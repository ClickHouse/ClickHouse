//
// SharedMemoryTest.h
//
// $Id: //poco/Main/template/test.h#7 $
//
// Definition of the SharedMemoryTest class.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef SharedMemoryTest_INCLUDED
#define SharedMemoryTest_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Path.h"
#include "CppUnit/TestCase.h"


class SharedMemoryTest: public CppUnit::TestCase
{
public:
	SharedMemoryTest(const std::string& name);
	~SharedMemoryTest();

	void testCreate();
	void testCreateFromFile();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

	static Poco::Path findDataFile(const std::string& afile);

private:
};


#endif // SharedMemoryTest_INCLUDED
