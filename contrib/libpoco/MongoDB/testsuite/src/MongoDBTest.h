//
// MongoDBTest.h
//
// $Id$
//
// Definition of the MongoDBTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MongoDBTest_INCLUDED
#define MongoDBTest_INCLUDED


#include "Poco/MongoDB/MongoDB.h"
#include "Poco/MongoDB/Connection.h"

#include "CppUnit/TestCase.h"


class MongoDBTest: public CppUnit::TestCase
{
public:
	MongoDBTest(const std::string& name);


	virtual ~MongoDBTest();

	void testInsertRequest();
	void testQueryRequest();
	void testDBQueryRequest();
	void testCountCommand();
	void testDBCountCommand();
	void testDBCount2Command();
	void testDeleteRequest();
	void testBuildInfo();
	void testConnectionPool();
	void testCursorRequest();
	void testObjectID();
	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:

	std::string _host;
	unsigned    _port;
	static bool _connected;
	static Poco::MongoDB::Connection _mongo;

};


#endif // MongoDBTest_INCLUDED
