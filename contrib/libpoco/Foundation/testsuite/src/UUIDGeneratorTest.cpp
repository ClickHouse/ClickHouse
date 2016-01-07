//
// UUIDGeneratorTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/UUIDGeneratorTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "UUIDGeneratorTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/UUIDGenerator.h"
#include "Poco/UUID.h"
#include "Poco/SHA1Engine.h"
#include <set>


using Poco::UUIDGenerator;
using Poco::UUID;


UUIDGeneratorTest::UUIDGeneratorTest(const std::string& name): CppUnit::TestCase(name)
{
}


UUIDGeneratorTest::~UUIDGeneratorTest()
{
}


void UUIDGeneratorTest::testTimeBased()
{
	UUIDGenerator& gen = UUIDGenerator::defaultGenerator();
	
	std::set<UUID> uuids;
	for (int i = 0; i < 1000; ++i)
	{
		UUID uuid = gen.create();
		assert (uuid.version() == UUID::UUID_TIME_BASED);
		assert (uuids.find(uuid) == uuids.end());
		uuids.insert(uuid);
	}
}


void UUIDGeneratorTest::testRandom()
{
	UUIDGenerator& gen = UUIDGenerator::defaultGenerator();

	std::set<UUID> uuids;
	for (int i = 0; i < 1000; ++i)
	{
		UUID uuid = gen.createRandom();
		assert (uuid.version() == UUID::UUID_RANDOM);
		assert (uuids.find(uuid) == uuids.end());
		uuids.insert(uuid);
	}
}


void UUIDGeneratorTest::testNameBased()
{
	UUIDGenerator& gen = UUIDGenerator::defaultGenerator();

	UUID uuid1 = gen.createFromName(UUID::uri(), "http://www.appinf.com/uuid");
	assert (uuid1.version() == UUID::UUID_NAME_BASED);
	assert (uuid1.variant() == 2);

	UUID uuid2 = gen.createFromName(UUID::uri(), "http://www.appinf.com/uuid2");
	assert (uuid2 != uuid1);

	UUID uuid3 = gen.createFromName(UUID::dns(), "www.appinf.com");
	assert (uuid3 != uuid1);

	UUID uuid4 = gen.createFromName(UUID::oid(), "1.3.6.1.4.1");
	assert (uuid4 != uuid1);

	UUID uuid5 = gen.createFromName(UUID::x500(), "cn=Guenter Obiltschnig, ou=People, o=Applied Informatics, c=at");
	assert (uuid5 != uuid1);

	UUID uuid6 = gen.createFromName(UUID::uri(), "http://www.appinf.com/uuid");
	assert (uuid6 == uuid1);

	Poco::SHA1Engine sha1;
	UUID uuid7 = gen.createFromName(UUID::uri(), "http://www.appinf.com/uuid", sha1);
	assert (uuid7.version() == UUID::UUID_NAME_BASED_SHA1);
	assert (uuid7.variant() == 2);
	assert (uuid7 != uuid1);
}


void UUIDGeneratorTest::setUp()
{
}


void UUIDGeneratorTest::tearDown()
{
}


CppUnit::Test* UUIDGeneratorTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("UUIDGeneratorTest");

	CppUnit_addTest(pSuite, UUIDGeneratorTest, testTimeBased);
	CppUnit_addTest(pSuite, UUIDGeneratorTest, testRandom);
	CppUnit_addTest(pSuite, UUIDGeneratorTest, testNameBased);

	return pSuite;
}
