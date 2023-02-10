//
// Database.h
//
// Library: MongoDB
// Package: MongoDB
// Module:  Database
//
// Definition of the Database class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MongoDB_Database_INCLUDED
#define MongoDB_Database_INCLUDED


#include "Poco/MongoDB/MongoDB.h"
#include "Poco/MongoDB/Connection.h"
#include "Poco/MongoDB/Document.h"
#include "Poco/MongoDB/QueryRequest.h"
#include "Poco/MongoDB/InsertRequest.h"
#include "Poco/MongoDB/UpdateRequest.h"
#include "Poco/MongoDB/DeleteRequest.h"


namespace Poco {
namespace MongoDB {


class MongoDB_API Database
	/// Database is a helper class for creating requests. MongoDB works with
	/// collection names and uses the part before the first dot as the name of
	/// the database.
{
public:
	explicit Database(const std::string& name);
		/// Creates a Database for the database with the given name.

	virtual ~Database();
		/// Destroys the Database.
		
	bool authenticate(Connection& connection, const std::string& username, const std::string& password, const std::string& method = AUTH_SCRAM_SHA1);
		/// Authenticates against the database using the given connection,
		/// username and password, as well as authentication method.
		///
		/// "MONGODB-CR" (default prior to MongoDB 3.0) and 
		/// "SCRAM-SHA-1" (default starting in 3.0) are the only supported 
		/// authentication methods.
		///
		/// Returns true if authentication was successful, otherwise false.
		///
		/// May throw a Poco::ProtocolException if authentication fails for a reason other than
		/// invalid credentials.

	Int64 count(Connection& connection, const std::string& collectionName) const;
		/// Sends a count request for the given collection to MongoDB. 
		///
		/// If the command fails, -1 is returned.

	Poco::SharedPtr<Poco::MongoDB::QueryRequest> createCommand() const;
		/// Creates a QueryRequest for a command.

	Poco::SharedPtr<Poco::MongoDB::QueryRequest> createCountRequest(const std::string& collectionName) const;
		/// Creates a QueryRequest to count the given collection. 
		/// The collectionname must not contain the database name.

	Poco::SharedPtr<Poco::MongoDB::DeleteRequest> createDeleteRequest(const std::string& collectionName) const;
		/// Creates a DeleteRequest to delete documents in the given collection.
		/// The collectionname must not contain the database name.

	Poco::SharedPtr<Poco::MongoDB::InsertRequest> createInsertRequest(const std::string& collectionName) const;
		/// Creates an InsertRequest to insert new documents in the given collection.
		/// The collectionname must not contain the database name.

	Poco::SharedPtr<Poco::MongoDB::QueryRequest> createQueryRequest(const std::string& collectionName) const;
		/// Creates a QueryRequest. 
		/// The collectionname must not contain the database name.

	Poco::SharedPtr<Poco::MongoDB::UpdateRequest> createUpdateRequest(const std::string& collectionName) const;
		/// Creates an UpdateRequest. 
		/// The collectionname must not contain the database name.

	Poco::MongoDB::Document::Ptr ensureIndex(Connection& connection,
		const std::string& collection,
		const std::string& indexName,
		Poco::MongoDB::Document::Ptr keys,
		bool unique = false,
		bool background = false,
		int version = 0,
		int ttl = 0);
		/// Creates an index. The document returned is the result of a getLastError call.
		/// For more info look at the ensureIndex information on the MongoDB website.

	Document::Ptr getLastErrorDoc(Connection& connection) const;
		/// Sends the getLastError command to the database and returns the error document.

	std::string getLastError(Connection& connection) const;
		/// Sends the getLastError command to the database and returns the err element
		/// from the error document. When err is null, an empty string is returned.

	static const std::string AUTH_MONGODB_CR;
		/// Default authentication mechanism prior to MongoDB 3.0.
		
	static const std::string AUTH_SCRAM_SHA1;
		/// Default authentication mechanism for MongoDB 3.0.

protected:
	bool authCR(Connection& connection, const std::string& username, const std::string& password);
	bool authSCRAM(Connection& connection, const std::string& username, const std::string& password);

private:
	std::string _dbname;
};


//
// inlines
//
inline Poco::SharedPtr<Poco::MongoDB::QueryRequest> Database::createCommand() const
{
	Poco::SharedPtr<Poco::MongoDB::QueryRequest> cmd = createQueryRequest("$cmd");
	cmd->setNumberToReturn(1);
	return cmd;
}


inline Poco::SharedPtr<Poco::MongoDB::DeleteRequest>
Database::createDeleteRequest(const std::string& collectionName) const
{
	return new Poco::MongoDB::DeleteRequest(_dbname + '.' + collectionName);
}


inline Poco::SharedPtr<Poco::MongoDB::InsertRequest>
Database::createInsertRequest(const std::string& collectionName) const
{
	return new Poco::MongoDB::InsertRequest(_dbname + '.' + collectionName);
}


inline Poco::SharedPtr<Poco::MongoDB::QueryRequest>
Database::createQueryRequest(const std::string& collectionName) const
{
	return new Poco::MongoDB::QueryRequest(_dbname + '.' + collectionName);
}


inline Poco::SharedPtr<Poco::MongoDB::UpdateRequest>
Database::createUpdateRequest(const std::string& collectionName) const
{
	return new Poco::MongoDB::UpdateRequest(_dbname + '.' + collectionName);
}


} } // namespace Poco::MongoDB


#endif // MongoDB_Database_INCLUDED
