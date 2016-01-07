//
// InsertRequest.h
//
// $Id$
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


class MongoDB_API InsertRequest : public RequestMessage
	/// Class for creating an OP_INSERT client request. This request is used
	/// to insert one or more documents to the database.
{
public:
	typedef enum
	{
		INSERT_NONE = 0,
		INSERT_CONTINUE_ON_ERROR = 1
	} Flags;

	InsertRequest(const std::string& collectionName, Flags flags = INSERT_NONE );
		/// Constructor.
		/// The full collection name is the concatenation of the database 
		/// name with the collection name, using a "." for the concatenation. For example, 
		/// for the database "foo" and the collection "bar", the full collection name is 
		/// "foo.bar".

	virtual ~InsertRequest();
		/// Destructor

	Document& addNewDocument();
		/// Adds a new document for insertion. A reference to the empty document is
		/// returned. InsertRequest is the owner of the Document and will free it
		/// on destruction.

	Document::Vector& documents();
		/// Returns the documents to insert into the database

protected:

	void buildRequest(BinaryWriter& writer);

private:

	Int32 _flags;
	std::string _fullCollectionName;
	Document::Vector _documents;
};


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


#endif //MongoDB_InsertRequest_INCLUDED
