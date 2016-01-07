//
// MongoDBTest.cpp
//
// $Id$
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//
#include <iostream>

#include "Poco/DateTime.h"
#include "Poco/ObjectPool.h"

#include "Poco/MongoDB/InsertRequest.h"
#include "Poco/MongoDB/QueryRequest.h"
#include "Poco/MongoDB/DeleteRequest.h"
#include "Poco/MongoDB/GetMoreRequest.h"
#include "Poco/MongoDB/PoolableConnectionFactory.h"
#include "Poco/MongoDB/Database.h"
#include "Poco/MongoDB/Cursor.h"
#include "Poco/MongoDB/ObjectId.h"

#include "Poco/Net/NetException.h"

#include "MongoDBTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"

using namespace Poco::MongoDB;


bool MongoDBTest::_connected = false;
Poco::MongoDB::Connection MongoDBTest::_mongo;


MongoDBTest::MongoDBTest(const std::string& name): 
	CppUnit::TestCase("MongoDB"),
	_host("localhost"),
	_port(27017)
{
	if (!_connected)
	{
		try
		{
			_mongo.connect(_host, _port);
			_connected = true;
			std::cout << "Connected to [" << _host << ':' << _port << ']' << std::endl;
		}
		catch (Poco::Net::ConnectionRefusedException& e)
		{
			std::cout << "Couldn't connect to " << e.message() << ". " << std::endl;
		}
	}
}


MongoDBTest::~MongoDBTest()
{
	if (_connected)
	{
		_mongo.disconnect();
		_connected = false;
		std::cout << "Disconnected from [" << _host << ':' << _port << ']' << std::endl;
	}
}


void MongoDBTest::setUp()
{
	
}


void MongoDBTest::tearDown()
{
}


void MongoDBTest::testInsertRequest()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	Poco::MongoDB::Document::Ptr player = new Poco::MongoDB::Document();
	player->add("lastname", std::string("Braem"));
	player->add("firstname", std::string("Franky"));

	Poco::DateTime birthdate;
	birthdate.assign(1969, 3, 9);
	player->add("birthdate", birthdate.timestamp());

	player->add("start", 1993);
	player->add("active", false);

	Poco::DateTime now;
	std::cout << now.day() << " " << now.hour() << ":" << now.minute() << ":" << now.second() << std::endl;
	player->add("lastupdated", now.timestamp());

	player->add("unknown", NullValue());

	Poco::MongoDB::InsertRequest request("team.players");
	request.documents().push_back(player);
	_mongo.sendRequest(request);
}

void MongoDBTest::testQueryRequest()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	Poco::MongoDB::QueryRequest request("team.players");
	request.selector().add("lastname" , std::string("Braem"));
	request.setNumberToReturn(1);

	Poco::MongoDB::ResponseMessage response;

	_mongo.sendRequest(request, response);

	if ( response.documents().size() > 0 )
	{
		Poco::MongoDB::Document::Ptr doc = response.documents()[0];

		try
		{
			std::string lastname = doc->get<std::string>("lastname");
			assert(lastname.compare("Braem") == 0);
			std::string firstname = doc->get<std::string>("firstname");
			assert(firstname.compare("Franky") == 0);
			Poco::Timestamp birthDateTimestamp = doc->get<Poco::Timestamp>("birthdate");
			Poco::DateTime birthDate(birthDateTimestamp);
			assert(birthDate.year() == 1969 && birthDate.month() == 3 && birthDate.day() == 9);
			Poco::Timestamp lastupdatedTimestamp = doc->get<Poco::Timestamp>("lastupdated");
			assert(doc->isType<NullValue>("unknown"));
			bool active = doc->get<bool>("active");
			assert(!active);

			std::string id = doc->get("_id")->toString();
			std::cout << id << std::endl;
		}
		catch(Poco::NotFoundException& nfe)
		{
			fail(nfe.message() + " not found.");
		}
	}
	else
	{
		fail("No document returned");
	}
}

void MongoDBTest::testDBQueryRequest()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	Database db("team");
	Poco::SharedPtr<Poco::MongoDB::QueryRequest> request = db.createQueryRequest("players");
	request->selector().add("lastname" , std::string("Braem"));

	Poco::MongoDB::ResponseMessage response;
	_mongo.sendRequest(*request, response);

	if ( response.documents().size() > 0 )
	{
		Poco::MongoDB::Document::Ptr doc = response.documents()[0];

		try
		{
			std::string lastname = doc->get<std::string>("lastname");
			assert(lastname.compare("Braem") == 0);
			std::string firstname = doc->get<std::string>("firstname");
			assert(firstname.compare("Franky") == 0);
			Poco::Timestamp birthDateTimestamp = doc->get<Poco::Timestamp>("birthdate");
			Poco::DateTime birthDate(birthDateTimestamp);
			assert(birthDate.year() == 1969 && birthDate.month() == 3 && birthDate.day() == 9);
			Poco::Timestamp lastupdatedTimestamp = doc->get<Poco::Timestamp>("lastupdated");
			assert(doc->isType<NullValue>("unknown"));

			std::string id = doc->get("_id")->toString();
			std::cout << id << std::endl;
		}
		catch(Poco::NotFoundException& nfe)
		{
			fail(nfe.message() + " not found.");
		}
	}
	else
	{
		fail("No document returned");
	}
}


void MongoDBTest::testCountCommand()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	Poco::MongoDB::QueryRequest request("team.$cmd");
	request.setNumberToReturn(1);
	request.selector().add("count", std::string("players"));

	Poco::MongoDB::ResponseMessage response;

	_mongo.sendRequest(request, response);

	if ( response.documents().size() > 0 )
	{
		Poco::MongoDB::Document::Ptr doc = response.documents()[0];
		double count = doc->get<double>("n");
		assert(count == 1);
	}
	else
	{
		fail("Didn't get a response from the count command");
	}
}


void MongoDBTest::testDBCountCommand()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	Poco::MongoDB::Database db("team");
	Poco::SharedPtr<Poco::MongoDB::QueryRequest> request = db.createCountRequest("players");

	Poco::MongoDB::ResponseMessage response;
	_mongo.sendRequest(*request, response);

	if ( response.documents().size() > 0 )
	{
		Poco::MongoDB::Document::Ptr doc = response.documents()[0];
		double count = doc->get<double>("n");
		assert(count == 1);
	}
	else
	{
		fail("Didn't get a response from the count command");
	}
}


void MongoDBTest::testDBCount2Command()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	Poco::MongoDB::Database db("team");
	double count = db.count(_mongo, "players");
	assert(count == 1);
}


void MongoDBTest::testDeleteRequest()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	Poco::MongoDB::DeleteRequest request("team.players");
	request.selector().add("lastname", std::string("Braem"));

	_mongo.sendRequest(request);
}


void MongoDBTest::testCursorRequest()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	Poco::MongoDB::Database db("team");
	Poco::SharedPtr<Poco::MongoDB::InsertRequest> insertRequest = db.createInsertRequest("numbers");
	for(int i = 0; i < 10000; ++i)
	{
		Document::Ptr doc = new Document();
		doc->add("number", i);
		insertRequest->documents().push_back(doc);
	}
	_mongo.sendRequest(*insertRequest);

	double count = db.count(_mongo, "numbers");
	assert(count == 10000);

	Poco::MongoDB::Cursor cursor("team", "numbers");

	int n = 0;
	Poco::MongoDB::ResponseMessage& response = cursor.next(_mongo);
	while(1)
	{
		n += response.documents().size();
		if ( response.cursorID() == 0 )
			break;
		response = cursor.next(_mongo);
	}
	std::cout << "n= " << n << std::endl;
	assert(n == 10000);

	Poco::MongoDB::QueryRequest drop("team.$cmd");
	drop.setNumberToReturn(1);
	drop.selector().add("drop", std::string("numbers"));

	Poco::MongoDB::ResponseMessage responseDrop;
	_mongo.sendRequest(drop, responseDrop);

	if ( responseDrop.documents().size() > 0 )
	{
		std::cout << responseDrop.documents()[0]->toString(2) << std::endl;
	}
}


void MongoDBTest::testBuildInfo()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	Poco::MongoDB::QueryRequest request("team.$cmd");
	request.setNumberToReturn(1);
	request.selector().add("buildInfo", 1);

	Poco::MongoDB::ResponseMessage response;

	try
	{
		_mongo.sendRequest(request, response);
	}
	catch(Poco::NotImplementedException& nie)
	{
		std::cout << nie.message() << std::endl;
		return;
	}

	if ( response.documents().size() > 0 )
	{
		Poco::MongoDB::Document::Ptr doc = response.documents()[0];
		std::cout << doc->toString(2);
	}
	else
	{
		fail("Didn't get a response from the buildinfo command");
	}
}


void MongoDBTest::testConnectionPool()
{
	Poco::Net::SocketAddress sa(_host, _port);
	Poco::PoolableObjectFactory<Poco::MongoDB::Connection, Poco::MongoDB::Connection::Ptr> factory(sa);
	Poco::ObjectPool<Poco::MongoDB::Connection, Poco::MongoDB::Connection::Ptr> pool(factory, 10, 15);

	Poco::MongoDB::PooledConnection pooledConnection(pool);

	Poco::MongoDB::QueryRequest request("team.$cmd");
	request.setNumberToReturn(1);
	request.selector().add("count", std::string("players"));

	Poco::MongoDB::ResponseMessage response;
	((Connection::Ptr) pooledConnection)->sendRequest(request, response);

	if ( response.documents().size() > 0 )
	{
		Poco::MongoDB::Document::Ptr doc = response.documents()[0];
		double count = doc->get<double>("n");
		assert(count == 1);
	}
	else
	{
		fail("Didn't get a response from the count command");
	}
}


void MongoDBTest::testObjectID()
{
	ObjectId oid("536aeebba081de6815000002");
	std::string str2 = oid.toString();
	assert(str2 == "536aeebba081de6815000002");
}


CppUnit::Test* MongoDBTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("MongoDBTest");

	CppUnit_addTest(pSuite, MongoDBTest, testInsertRequest);
	CppUnit_addTest(pSuite, MongoDBTest, testQueryRequest);
	CppUnit_addTest(pSuite, MongoDBTest, testDBQueryRequest);
	CppUnit_addTest(pSuite, MongoDBTest, testCountCommand);
	CppUnit_addTest(pSuite, MongoDBTest, testDBCountCommand);
	CppUnit_addTest(pSuite, MongoDBTest, testDBCount2Command);
	CppUnit_addTest(pSuite, MongoDBTest, testConnectionPool);
	CppUnit_addTest(pSuite, MongoDBTest, testDeleteRequest);
	CppUnit_addTest(pSuite, MongoDBTest, testBuildInfo);
	CppUnit_addTest(pSuite, MongoDBTest, testCursorRequest);
	CppUnit_addTest(pSuite, MongoDBTest, testObjectID);

	return pSuite;
}
