//
// SharedMemoryTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/SharedMemoryTest.cpp#1 $
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "SharedMemoryTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/SharedMemory.h"
#include "Poco/Path.h"
#include "Poco/File.h"
#include "Poco/Exception.h"


using Poco::SharedMemory;


SharedMemoryTest::SharedMemoryTest(const std::string& name): CppUnit::TestCase(name)
{
}


SharedMemoryTest::~SharedMemoryTest()
{
}


void SharedMemoryTest::testCreate()
{
	SharedMemory mem("hi", 4096, SharedMemory::AM_WRITE);
	assert (mem.end()-mem.begin() == 4096);
	mem.begin()[0] = 'A';
	mem.end()[-1] = 'Z';
}


void SharedMemoryTest::testCreateFromFile()
{
	Poco::Path p = findDataFile("testdata.txt");
	Poco::File f(p);
	assert (f.exists() && f.isFile());
	SharedMemory mem(f, SharedMemory::AM_READ);
	assert (mem.end() > mem.begin()); // valid?
	assert (mem.end() - mem.begin() == f.getSize());
	assert (mem.begin()[0] == 'A');
	assert (mem.end()[-5] == 'Z');
}


Poco::Path SharedMemoryTest::findDataFile(const std::string& afile)
{
	Poco::Path root;
	root.makeAbsolute();
	Poco::Path result;
	while (!Poco::Path::find(root.toString(), "data", result))
	{
		root.makeParent();
		if (root.toString().empty() || root.toString() == "/" || root.toString() == "\\")
			throw Poco::FileNotFoundException("Didn't find data subdir");
	}
	result.makeDirectory();
	result.setFileName(afile);
	Poco::File aFile(result.toString());
	if (!aFile.exists() || (aFile.exists() && !aFile.isFile()))
		throw Poco::FileNotFoundException("Didn't find file " + afile);
	
	return result;
}


void SharedMemoryTest::setUp()
{
}


void SharedMemoryTest::tearDown()
{
}


CppUnit::Test* SharedMemoryTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("SharedMemoryTest");

	CppUnit_addTest(pSuite, SharedMemoryTest, testCreate);
	CppUnit_addTest(pSuite, SharedMemoryTest, testCreateFromFile);

	return pSuite;
}
