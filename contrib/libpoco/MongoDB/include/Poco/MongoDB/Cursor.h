//
// Cursor.h
//
// $Id$
//
// Library: MongoDB
// Package: MongoDB
// Module:  Cursor
//
// Definition of the Cursor class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MongoDB_Cursor_INCLUDED
#define MongoDB_Cursor_INCLUDED


#include "Poco/MongoDB/MongoDB.h"
#include "Poco/MongoDB/Connection.h"
#include "Poco/MongoDB/QueryRequest.h"
#include "Poco/MongoDB/ResponseMessage.h"


namespace Poco {
namespace MongoDB {


class MongoDB_API Cursor : public Document
	/// Cursor is an helper class for querying multiple documents
{
public:
	Cursor(const std::string& dbname, const std::string& collectionName, QueryRequest::Flags flags = QueryRequest::QUERY_NONE);
		/// Constructor

	Cursor(const std::string& fullCollectionName, QueryRequest::Flags flags = QueryRequest::QUERY_NONE);
		/// Constructor

	virtual ~Cursor();
		/// Destructor

	ResponseMessage& next(Connection& connection);
		/// Try to get the next documents. As long as ResponseMessage has a
		/// cursor id next can be called to retrieve the next bunch of documents.
		/// kill must be called when not all documents are needed.

	QueryRequest& query();
		/// Returns the associated query

	void kill(Connection& connection);
		/// Kills the cursor and reset it so that it can be reused.

private:
	QueryRequest    _query;
	ResponseMessage _response;
};


inline QueryRequest& Cursor::query()
{
	return _query;
}


} } // namespace Poco::MongoDB


#endif //MongoDB_Cursor_INCLUDED
