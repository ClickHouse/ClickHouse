//
// DirectoryIteratorsTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/DirectoryIteratorsTest.cpp#1 $
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DirectoryIteratorsTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/DirectoryIterator.h"
#include "Poco/SortedDirectoryIterator.h"
#include "Poco/RecursiveDirectoryIterator.h"
#include "Poco/FileStream.h"

#include <iostream>

using namespace Poco;


DirectoryIteratorsTest::DirectoryIteratorsTest(const std::string& name):
	CppUnit::TestCase(name)
{
}


DirectoryIteratorsTest::~DirectoryIteratorsTest()
{
}


void DirectoryIteratorsTest::testDirectoryIterator()
{
	Path p = path();
	DirectoryIterator dirIterator(p);
	DirectoryIterator end;
	std::vector<std::string> result;
	std::string file;

	while (dirIterator != end)
	{
		file = dirIterator->path();
		++dirIterator;
		result.push_back(file);
	}

	assertEquals(7, (long) result.size());
}


void DirectoryIteratorsTest::testSortedDirectoryIterator()
{
	Path p = path();
	SortedDirectoryIterator dirIterator(p);
	SortedDirectoryIterator end;
	std::vector<std::string> result;
	std::string file;

	while (dirIterator != end)
	{
		file = Path(dirIterator->path()).getFileName();
		++dirIterator;
		result.push_back(file);
	}

	assertEquals(7, (long) result.size());
	assertEquals("first", result[0]);
	assertEquals("1", result[1]);
	assertEquals("2", result[2]);
	assertEquals("A", result[3]);
	assertEquals("B", result[4]);
	assertEquals("c", result[5]);
	assertEquals("d", result[6]);
}


void DirectoryIteratorsTest::testSimpleRecursiveDirectoryIterator()
{
	Path p = path();
	SimpleRecursiveDirectoryIterator dirIterator(p);
	SimpleRecursiveDirectoryIterator end;
	std::vector<std::string> result;
	std::string file;

	while (dirIterator != end)
	{
		file = dirIterator->path();
		++dirIterator;
		result.push_back(file);
	}

	assertEquals(20, (long) result.size());
}


void DirectoryIteratorsTest::testSiblingsFirstRecursiveDirectoryIterator()
{
	Path p = path();
	SiblingsFirstRecursiveDirectoryIterator dirIterator(p);
	SiblingsFirstRecursiveDirectoryIterator end;
	std::vector<std::string> result;
	std::string file;

	while (dirIterator != end)
	{
		file = dirIterator->path();
		++dirIterator;
		result.push_back(file);
	}

	assertEquals(20, (long) result.size());
}


void DirectoryIteratorsTest::setUp()
{
	File d(path());
	if (d.exists()) d.remove(true);

	/*
	  Build Directory Tree like this:

	  DirectoryIteratorsTest
		|-- 1
		|-- 2
		|-- A
		|-- B
		|-- c
		|-- d
		`-- first
			|-- 1
			|-- 2
			|-- A
			|-- B
			|-- c
			|-- d
			`-- second
				|-- 1
				|-- 2
				|-- A
				|-- B
				|-- c
				`-- d

		2 directories, 18 files
	 */
	Path p = path();
	createSubdir(p);

	p.pushDirectory("first");
	createSubdir(p);

	p.pushDirectory("second");
	createSubdir(p);
}


void DirectoryIteratorsTest::createSubdir(Path& p)
{
	File d(p);
	d.createDirectories();
	FileStream f1(p.toString() + "d");
	FileStream f2(p.toString() + "1");
	FileStream f3(p.toString() + "A");
	FileStream f4(p.toString() + "2");
	FileStream f5(p.toString() + "B");
	FileStream f6(p.toString() + "c");
}


void DirectoryIteratorsTest::tearDown()
{
	try
	{
		File d(path());
		d.remove(true);
	}
	catch (...)
	{
	}
}


Poco::Path DirectoryIteratorsTest::path() const
{
	Path p(Path::current());
	p.pushDirectory("DirectoryIteratorsTest");
	return p;
}


CppUnit::Test* DirectoryIteratorsTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("DirectoryIteratorsTest");

	CppUnit_addTest(pSuite, DirectoryIteratorsTest, testDirectoryIterator);
	CppUnit_addTest(pSuite, DirectoryIteratorsTest, testSortedDirectoryIterator);
	CppUnit_addTest(pSuite, DirectoryIteratorsTest, testSimpleRecursiveDirectoryIterator);
	CppUnit_addTest(pSuite, DirectoryIteratorsTest, testSiblingsFirstRecursiveDirectoryIterator);

	return pSuite;
}
