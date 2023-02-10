//
// DeleteRequest.h
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


class MongoDB_API DeleteRequest: public RequestMessage
	/// A DeleteRequest is used to delete one ore more documents from a database.
	///
	/// Specific flags for this request
	///   - DELETE_DEFAULT: default delete operation
	///   - DELETE_SINGLE_REMOVE: delete only the first document
{
public:
	enum Flags
	{
		DELETE_DEFAULT = 0,
			/// Default

		DELETE_SINGLE_REMOVE = 1
			/// Delete only the first document.
	};

	DeleteRequest(const std::string& collectionName, Flags flags = DELETE_DEFAULT);
		/// Creates a DeleteRequest for the given collection using the given flags. 
		///
		/// The full collection name is the concatenation of the database 
		/// name with the collection name, using a "." for the concatenation. For example, 
		/// for the database "foo" and the collection "bar", the full collection name is 
		/// "foo.bar".

	DeleteRequest(const std::string& collectionName, bool justOne);
		/// Creates a DeleteRequest for the given collection.
		/// 
		/// The full collection name is the concatenation of the database
		/// name with the collection name, using a "." for the concatenation. For example,
		/// for the database "foo" and the collection "bar", the full collection name is
		/// "foo.bar". 
		///
		/// If justOne is true, only the first matching document will
		/// be removed (the same as using flag DELETE_SINGLE_REMOVE).

	virtual ~DeleteRequest();
		/// Destructor

	Flags flags() const;
		/// Returns the flags.

	void flags(Flags flag);
		/// Sets the flags.

	Document& selector();
		/// Returns the selector document.

protected:
	void buildRequest(BinaryWriter& writer);
		/// Writes the OP_DELETE request to the writer.

private:
	Flags       _flags;
	std::string _fullCollectionName;
	Document    _selector;
};


///
/// inlines
///
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


#endif // MongoDB_DeleteRequest_INCLUDED
