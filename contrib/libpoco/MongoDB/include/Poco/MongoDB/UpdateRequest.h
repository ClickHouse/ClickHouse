//
// UpdateRequest.h
//
// $Id$
//
// Library: MongoDB
// Package: MongoDB
// Module:  UpdateRequest
//
// Definition of the UpdateRequest class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MongoDB_UpdateRequest_INCLUDED
#define MongoDB_UpdateRequest_INCLUDED


#include "Poco/MongoDB/MongoDB.h"
#include "Poco/MongoDB/RequestMessage.h"
#include "Poco/MongoDB/Document.h"


namespace Poco {
namespace MongoDB {


class UpdateRequest : public RequestMessage
	/// Class for creating an OP_UPDATE client request. This request is used
	/// to update a document.
{
public:
	typedef enum
	{
		UPDATE_NOFLAGS = 0,
		// If set, the database will insert the supplied object into the
		// collection if no matching document is found.
		UPDATE_UPSERT = 1,
		// if set, the database will update all matching objects in the collection.
		// Otherwise only updates first matching doc.
		UPDATE_MULTIUPDATE = 2
	} Flags;

	UpdateRequest(const std::string& collectionName, Flags flags = UPDATE_NOFLAGS);
		/// Constructor.
		/// The full collection name is the concatenation of the database 
		/// name with the collection name, using a "." for the concatenation. For example, 
		/// for the database "foo" and the collection "bar", the full collection name is 
		/// "foo.bar".

	virtual ~UpdateRequest();
		/// Destructor
		

	Document& selector();
		/// Returns the selector document
		

	Document& update();
		/// The document to update

	Flags flags() const;
		/// Returns the flags

	void flags(Flags flags);
		/// Sets the flags

protected:
	void buildRequest(BinaryWriter& writer);


private:
	Flags       _flags;
	std::string _fullCollectionName;
	Document    _selector;
	Document    _update;
};


inline UpdateRequest::Flags UpdateRequest::flags() const
{
	return _flags;
}

inline void UpdateRequest::flags(UpdateRequest::Flags flags)
{
	_flags = flags;
}

inline Document& UpdateRequest::selector()
{
	return _selector;
}

inline Document& UpdateRequest::update()
{
	return _update;
}


} } // namespace Poco::MongoDB


#endif //MongoDB_UpdateRequest_INCLUDED
