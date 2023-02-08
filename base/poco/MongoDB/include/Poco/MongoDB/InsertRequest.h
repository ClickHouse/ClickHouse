//
// InsertRequest.h
//
// Library: MongoDB
// Package: MongoDB
// Module:  InsertRequest
//
// Definition of the InsertRequest class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MongoDB_InsertRequest_INCLUDED
#define MongoDB_InsertRequest_INCLUDED


#include "Poco/MongoDB/MongoDB.h"
#include "Poco/MongoDB/RequestMessage.h"
#include "Poco/MongoDB/Document.h"


namespace Poco {
namespace MongoDB {


class MongoDB_API InsertRequest: public RequestMessage
	/// A request for inserting one or more documents to the database
	/// (OP_INSERT).
{
public:
	enum Flags
	{
		INSERT_DEFAULT = 0,
			/// If specified, perform a normal insert operation.
		
		INSERT_CONTINUE_ON_ERROR = 1
			/// If set, the database will not stop processing a bulk insert if one 
			/// fails (e.g. due to duplicate IDs). This makes bulk insert behave similarly 
			/// to a series of single inserts, except lastError will be set if any insert 
			/// fails, not just the last one. If multiple errors occur, only the most 
			/// recent will be reported.
	};

	InsertRequest(const std::string& collectionName, Flags flags = INSERT_DEFAULT);
		/// Creates an InsertRequest.
		///
		/// The full collection name is the concatenation of the database 
		/// name with the collection name, using a "." for the concatenation. For example, 
		/// for the database "foo" and the collection "bar", the full collection name is 
		/// "foo.bar".

	virtual ~InsertRequest();
		/// Destroys the InsertRequest.

	Document& addNewDocument();
		/// Adds a new document for insertion. A reference to the empty document is
		/// returned. InsertRequest is the owner of the Document and will free it
		/// on destruction.

	Document::Vector& documents();
		/// Returns the documents to insert into the database.

protected:
	void buildRequest(BinaryWriter& writer);

private:
	Int32 _flags;
	std::string _fullCollectionName;
	Document::Vector _documents;
};


//
// inlines
//
inline Document& InsertRequest::addNewDocument()
{
	Document::Ptr doc = new Document();
	_documents.push_back(doc);
	return *doc;
}


inline Document::Vector& InsertRequest::documents()
{
	return _documents;
}


} } // namespace Poco::MongoDB


#endif // MongoDB_InsertRequest_INCLUDED
