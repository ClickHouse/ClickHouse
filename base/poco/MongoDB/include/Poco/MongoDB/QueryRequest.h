//
// QueryRequest.h
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


class MongoDB_API QueryRequest: public RequestMessage
	/// A request to query documents in a MongoDB database
	/// using an OP_QUERY request.
{
public:
	enum Flags
	{
		QUERY_DEFAULT = 0, 
			/// Do not set any flags.
	
		QUERY_TAILABLE_CURSOR = 2, 
			/// Tailable means cursor is not closed when the last data is retrieved. 
			/// Rather, the cursor marks the final object’s position. 
			/// You can resume using the cursor later, from where it was located, 
			/// if more data were received. Like any "latent cursor", the cursor may 
			/// become invalid at some point (CursorNotFound) – for example if the final 
			/// object it references were deleted.
	
		QUERY_SLAVE_OK = 4, 
			/// Allow query of replica slave. Normally these return an error except 
			/// for namespace "local".
			
		// QUERY_OPLOG_REPLAY = 8 (internal replication use only - drivers should not implement)
		
		QUERY_NO_CURSOR_TIMEOUT = 16,
			/// The server normally times out idle cursors after an inactivity period 
			/// (10 minutes) to prevent excess memory use. Set this option to prevent that.
	
		QUERY_AWAIT_DATA = 32,
			/// Use with QUERY_TAILABLECURSOR. If we are at the end of the data, block for 
			/// a while rather than returning no data. After a timeout period, we do 
			/// return as normal.

		QUERY_EXHAUST = 64,
			/// Stream the data down full blast in multiple "more" packages, on the 
			/// assumption that the client will fully read all data queried. 
			/// Faster when you are pulling a lot of data and know you want to pull 
			/// it all down. 
			/// Note: the client is not allowed to not read all the data unless it 
			/// closes the connection.

		QUERY_PARTIAL = 128
			/// Get partial results from a mongos if some shards are down 
			/// (instead of throwing an error).
	};

	QueryRequest(const std::string& collectionName, Flags flags = QUERY_DEFAULT);
		/// Creates a QueryRequest.
		///
		/// The full collection name is the concatenation of the database 
		/// name with the collection name, using a "." for the concatenation. For example, 
		/// for the database "foo" and the collection "bar", the full collection name is 
		/// "foo.bar".

	virtual ~QueryRequest();
		/// Destroys the QueryRequest.

	Flags getFlags() const;
		/// Returns the flags.

	void setFlags(Flags flag);
		/// Set the flags.

	std::string fullCollectionName() const;
		/// Returns the <db>.<collection> used for this query.

	Int32 getNumberToSkip() const;
		/// Returns the number of documents to skip.

	void setNumberToSkip(Int32 n);
		/// Sets the number of documents to skip.

	Int32 getNumberToReturn() const;
		/// Returns the number of documents to return.

	void setNumberToReturn(Int32 n);
		/// Sets the number of documents to return (limit).

	Document& selector();
		/// Returns the selector document.

	Document& returnFieldSelector();
		/// Returns the field selector document.

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


//
// inlines
//
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


#endif // MongoDB_QueryRequest_INCLUDED
