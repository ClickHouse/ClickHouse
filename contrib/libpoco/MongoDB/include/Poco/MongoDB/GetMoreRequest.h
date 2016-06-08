//
// GetMoreRequest.h
//
// $Id$
//
// Library: MongoDB
// Package: MongoDB
// Module:  GetMoreRequest
//
// Definition of the GetMoreRequest class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MongoDB_GetMoreRequest_INCLUDED
#define MongoDB_GetMoreRequest_INCLUDED


#include "Poco/MongoDB/MongoDB.h"
#include "Poco/MongoDB/RequestMessage.h"


namespace Poco {
namespace MongoDB {


class MongoDB_API GetMoreRequest : public RequestMessage
	/// Class for creating an OP_GETMORE client request. This request is used
	/// to query the database for more documents in a collection after
	/// a query request is send.
{
public:
	GetMoreRequest(const std::string& collectionName, Int64 cursorID);
		/// Constructor. The full collection name is the concatenation of the database 
		/// name with the collection name, using a "." for the concatenation. For example, 
		/// for the database "foo" and the collection "bar", the full collection name is 
		/// "foo.bar". The cursorID has been returned by the response on the query request.
		/// By default the numberToReturn is set to 100.


	virtual ~GetMoreRequest();
		/// Destructor

	Int32 getNumberToReturn() const;
		/// Returns the limit of returned documents

	void setNumberToReturn(Int32 n);
		/// Sets the limit of returned documents

	Int64 cursorID() const;
		/// Returns the cursor id

protected:
	void buildRequest(BinaryWriter& writer);

private:
	Int32 _flags;
	std::string _fullCollectionName;
	Int32 _numberToReturn;
	Int64 _cursorID;
};


//
// inlines
//
inline Int32 GetMoreRequest::getNumberToReturn() const
{
	return _numberToReturn;
}


inline void GetMoreRequest::setNumberToReturn(Int32 n)
{
	_numberToReturn = n;
}


inline Int64 GetMoreRequest::cursorID() const
{
	return _cursorID;
}


} } // namespace Poco::MongoDB


#endif //MongoDB_GetMoreRequest_INCLUDED
