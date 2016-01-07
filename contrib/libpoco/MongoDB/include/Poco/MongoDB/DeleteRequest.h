//
// DeleteRequest.h
//
// $Id$
//
// Library: MongoDB
// Package: MongoDB
// Module:  DeleteRequest
//
// Definition of the DeleteRequest class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MongoDB_DeleteRequest_INCLUDED
#define MongoDB_DeleteRequest_INCLUDED


#include "Poco/MongoDB/MongoDB.h"
#include "Poco/MongoDB/RequestMessage.h"
#include "Poco/MongoDB/Document.h"


namespace Poco {
namespace MongoDB {


class MongoDB_API DeleteRequest : public RequestMessage
	/// Class for creating an OP_DELETE client request. This request
	/// is used to delete one ore more documents from a database.
	///
	/// Specific flags for this request
	///  - DELETE_NONE
	///      No flags
	///  - DELETE_SINGLE_REMOVE
	///      Delete only the first document
{
public:
	typedef enum
	{
		DELETE_NONE = 0,
		DELETE_SINGLE_REMOVE = 1
	} Flags;

	DeleteRequest(const std::string& collectionName, Flags flags = DELETE_NONE);
		/// Constructor. The full collection name is the concatenation of the database 
		/// name with the collection name, using a "." for the concatenation. For example, 
		/// for the database "foo" and the collection "bar", the full collection name is 
		/// "foo.bar".

	DeleteRequest(const std::string& collectionName, bool justOne);
		/// Constructor. The full collection name is the concatenation of the database
		/// name with the collection name, using a "." for the concatenation. For example,
		/// for the database "foo" and the collection "bar", the full collection name is
		/// "foo.bar". When justOne is true, only the first matching document will
		/// be removed (the same as using flag DELETE_SINGLE_REMOVE).

	virtual ~DeleteRequest();
		/// Destructor

	Flags flags() const;
		/// Returns flags

	void flags(Flags flag);
		/// Sets flags

	Document& selector();
		/// Returns the selector document

protected:
	void buildRequest(BinaryWriter& writer);
		/// Writes the OP_DELETE request to the writer

private:
	Flags       _flags;
	std::string _fullCollectionName;
	Document    _selector;
};


inline DeleteRequest::Flags DeleteRequest::flags() const
{
	return _flags;
}


inline void DeleteRequest::flags(DeleteRequest::Flags flags)
{
	_flags = flags;
}


inline Document& DeleteRequest::selector()
{
	return _selector;
}


} } // namespace Poco::MongoDB


#endif //MongoDB_DeleteRequest_INCLUDED
