//
// Database.cpp
//
// $Id$
//
// Library: MongoDB
// Package: MongoDB
// Module:  Database
//
// Implementation of the Database class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/MongoDB/Database.h"


namespace Poco {
namespace MongoDB {


Database::Database( const std::string& db) : _dbname(db)
{
}

Database::~Database()
{
}


double Database::count(Connection& connection, const std::string& collectionName) const
{
	Poco::SharedPtr<Poco::MongoDB::QueryRequest> countRequest = createCountRequest(collectionName);

	Poco::MongoDB::ResponseMessage response;
	connection.sendRequest(*countRequest, response);

	if ( response.documents().size() > 0 )
	{
		Poco::MongoDB::Document::Ptr doc = response.documents()[0];
		return doc->get<double>("n");
	}

	return -1;
}


Poco::MongoDB::Document::Ptr Database::ensureIndex(Connection& connection, const std::string& collection, const std::string& indexName, Poco::MongoDB::Document::Ptr keys, bool unique, bool background, int version, int ttl)
{
	Poco::MongoDB::Document::Ptr index = new Poco::MongoDB::Document();
	index->add("ns", _dbname + "." + collection);
	index->add("name", indexName);
	index->add("key", keys);

	if ( version > 0 )
	{
		index->add("version", version);
	}

	if ( unique )
	{
		index->add("unique", true);
	}

	if ( background )
	{
		index->add("background", true);
	}

	if ( ttl > 0 )
	{
		index->add("expireAfterSeconds", ttl);
	}

	Poco::SharedPtr<Poco::MongoDB::InsertRequest> insertRequest = createInsertRequest("system.indexes");
	insertRequest->documents().push_back(index);
	connection.sendRequest(*insertRequest);

	return getLastErrorDoc(connection);
}


Document::Ptr Database::getLastErrorDoc(Connection& connection) const
{
	Document::Ptr errorDoc;

	Poco::SharedPtr<Poco::MongoDB::QueryRequest> request = createQueryRequest("$cmd");
	request->setNumberToReturn(1);
	request->selector().add("getLastError", 1);

	Poco::MongoDB::ResponseMessage response;
	connection.sendRequest(*request, response);

	if ( response.documents().size() > 0 )
	{
		errorDoc = response.documents()[0];
	}

	return errorDoc;
}


std::string Database::getLastError(Connection& connection) const
{
	Document::Ptr errorDoc = getLastErrorDoc(connection);
	if ( !errorDoc.isNull() && errorDoc->isType<std::string>("err") )
	{
		return errorDoc->get<std::string>("err");
	}

	return "";
}


Poco::SharedPtr<Poco::MongoDB::QueryRequest> Database::createCountRequest(const std::string& collectionName) const
{
	Poco::SharedPtr<Poco::MongoDB::QueryRequest> request = createQueryRequest("$cmd");
	request->setNumberToReturn(1);
	request->selector().add("count", collectionName);
	return request;
}


} } // namespace Poco::MongoDB
