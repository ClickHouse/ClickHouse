//
// ManifestTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/ManifestTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "ManifestTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Manifest.h"
#include "Poco/MetaObject.h"
#include <set>


using Poco::Manifest;
using Poco::MetaObject;


class MfTestBase
{
};


class MfTestObject: public MfTestBase
{
};


ManifestTest::ManifestTest(const std::string& name): CppUnit::TestCase(name)
{
}


ManifestTest::~ManifestTest()
{
}


void ManifestTest::testManifest()
{
	Manifest<MfTestBase> manifest;
	assert (manifest.empty());
	assert (manifest.size() == 0);
	assert (manifest.insert(new MetaObject<MfTestObject, MfTestBase>("MfTestObject1")));
	assert (!manifest.empty());
	assert (manifest.size() == 1);
	assert (manifest.insert(new MetaObject<MfTestObject, MfTestBase>("MfTestObject2")));
	MetaObject<MfTestObject, MfTestBase>* pMeta = new MetaObject<MfTestObject, MfTestBase>("MfTestObject2");
	assert (!manifest.insert(pMeta));
	delete pMeta;
	assert (!manifest.empty());
	assert (manifest.size() == 2);
	assert (manifest.insert(new MetaObject<MfTestObject, MfTestBase>("MfTestObject3")));
	assert (manifest.size() == 3);

	assert (manifest.find("MfTestObject1") != manifest.end());
	assert (manifest.find("MfTestObject2") != manifest.end());
	assert (manifest.find("MfTestObject3") != manifest.end());
	assert (manifest.find("MfTestObject4") == manifest.end());
	
	std::set<std::string> classes;
	
	Manifest<MfTestBase>::Iterator it = manifest.begin();
	assert (it != manifest.end());
	classes.insert(it->name());
	++it;
	assert (it != manifest.end());
	classes.insert(it->name());
	++it;
	assert (it != manifest.end());
	classes.insert(it->name());
	it++;
	assert (it == manifest.end());
	
	assert (classes.find("MfTestObject1") != classes.end());
	assert (classes.find("MfTestObject2") != classes.end());
	assert (classes.find("MfTestObject3") != classes.end());
	
	manifest.clear();
	assert (manifest.empty());
	assert (manifest.size() == 0);
	assert (manifest.insert(new MetaObject<MfTestObject, MfTestBase>("MfTestObject4")));
	assert (!manifest.empty());
	assert (manifest.size() == 1);
	it = manifest.begin();
	assert (it != manifest.end());
	assert (std::string(it->name()) == "MfTestObject4");
	++it;
	assert (it == manifest.end());
}


void ManifestTest::setUp()
{
}


void ManifestTest::tearDown()
{
}


CppUnit::Test* ManifestTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("ManifestTest");

	CppUnit_addTest(pSuite, ManifestTest, testManifest);

	return pSuite;
}
