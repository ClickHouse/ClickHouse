//
// ResponseMessage.h
//
// $Id$
//
// Library: MongoDB
// Package: MongoDB
// Module:  ResponseMessage
//
// Definition of the ResponseMessage class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MongoDB_ResponseMessage_INCLUDED
#define MongoDB_ResponseMessage_INCLUDED


#include "Poco/MongoDB/MongoDB.h"
#include "Poco/MongoDB/Message.h"
#include "Poco/MongoDB/Document.h"
#include <istream>


namespace Poco {
namespace MongoDB {


class MongoDB_API ResponseMessage : public Message
	/// Class that represents a response (OP_REPLY) from MongoDB
{
public:
	ResponseMessage();
		/// Constructor

	virtual ~ResponseMessage();
		/// Destructor

	Int64 cursorID() const;
		/// Returns the cursor id

	void clear();
		/// Clears the response

	size_t count() const;
		/// Returns the number of documents in the response

	Document::Vector& documents();
		/// Returns the retrieved documents

	bool empty() const;
		/// Returns true when the response doesn't contain any documents

	bool hasDocuments() const;
		/// Returns true when there is at least one document

	void read(std::istream& istr);
		/// Reads the response from the stream

private:
	Int32 _responseFlags;
	Int64 _cursorID;
	Int32 _startingFrom;
	Int32 _numberReturned;
	Document::Vector _documents;
};


inline size_t ResponseMessage::count() const
{
	return _documents.size();
}


inline bool ResponseMessage::empty() const
{
	return _documents.size() == 0;
}


inline Int64 ResponseMessage::cursorID() const
{
	return _cursorID;
}


inline Document::Vector& ResponseMessage::documents()
{
	return _documents;
}


inline bool ResponseMessage::hasDocuments() const
{
	return _documents.size() > 0;
}


} } // namespace Poco::MongoDB


#endif //MongoDB_ResponseMessage_INCLUDED
