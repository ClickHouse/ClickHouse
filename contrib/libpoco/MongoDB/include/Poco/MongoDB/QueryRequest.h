//
// QueryRequest.h
//
// $Id$
//
// Library: MongoDB
// Package: MongoDB
// Module:  QueryRequest
//
// Definition of the QueryRequest class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MongoDB_QueryRequest_INCLUDED
#define MongoDB_QueryRequest_INCLUDED


#include "Poco/MongoDB/MongoDB.h"
#include "Poco/MongoDB/RequestMessage.h"
#include "Poco/MongoDB/Document.h"


namespace Poco {
namespace MongoDB {


class MongoDB_API QueryRequest : public RequestMessage
	/// Class for creating an OP_QUERY client request. This request
	/// is used to query documents from the database.
{
public:
	typedef enum
	{
		QUERY_NONE = 0, 
		QUERY_TAILABLECURSOR = 2, 
		QUERY_SLAVE_OK = 4, 
		//QUERY_OPLOG_REPLAY = 8 (internal replication use only - drivers should not implement)
		QUERY_NOCUROSR_TIMEOUT = 16,
		QUERY_AWAIT_DATA = 32,
		QUERY_EXHAUST = 64,
		QUERY_PARTIAL = 128
	} Flags;

	QueryRequest(const std::string& collectionName, Flags flags = QUERY_NONE);
		/// Constructor.
		/// The full collection name is the concatenation of the database 
		/// name with the collection name, using a "." for the concatenation. For example, 
		/// for the database "foo" and the collection "bar", the full collection name is 
		/// "foo.bar".

	virtual ~QueryRequest();
		/// Destructor

	Flags getFlags() const;
		/// Returns the flags

	void setFlags(Flags flag);
		/// Set the flags

	std::string fullCollectionName() const;
		/// Returns the <db>.<collection> used for this query

	Int32 getNumberToSkip() const;
		/// Returns the number of documents to skip

	void setNumberToSkip(Int32 n);
		/// Sets the number of documents to skip

	Int32 getNumberToReturn() const;
		/// Returns the number to return

	void setNumberToReturn(Int32 n);
		/// Sets the number to return (limit)

	Document& selector();
		/// Returns the selector document

	Document& returnFieldSelector();
		/// Returns the field selector document

protected:
	void buildRequest(BinaryWriter& writer);

private:
	Flags       _flags;
	std::string _fullCollectionName;
	Int32       _numberToSkip;
	Int32       _numberToReturn;
	Document    _selector;
	Document    _returnFieldSelector;
};


inline QueryRequest::Flags QueryRequest::getFlags() const
{
	return _flags;
}


inline void QueryRequest::setFlags(QueryRequest::Flags flags)
{
	_flags = flags;
}


inline std::string QueryRequest::fullCollectionName() const
{
	return _fullCollectionName;
}


inline Document& QueryRequest::selector()
{
	return _selector;
}


inline Document& QueryRequest::returnFieldSelector()
{
	return _returnFieldSelector;
}


inline Int32 QueryRequest::getNumberToSkip() const
{
	return _numberToSkip;
}


inline void QueryRequest::setNumberToSkip(Int32 n)
{
	_numberToSkip = n;
}


inline Int32 QueryRequest::getNumberToReturn() const
{
	return _numberToReturn;
}


inline void QueryRequest::setNumberToReturn(Int32 n)
{
	_numberToReturn = n;
}


} } // namespace Poco::MongoDB


#endif //MongoDB_QueryRequest_INCLUDED
