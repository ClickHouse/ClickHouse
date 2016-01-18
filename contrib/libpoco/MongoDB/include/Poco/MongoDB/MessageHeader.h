//
// MessageHeader.h
//
// $Id$
//
// Library: MongoDB
// Package: MongoDB
// Module:  MessageHeader
//
// Definition of the MessageHeader class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MongoDB_MessageHeader_INCLUDED
#define MongoDB_MessageHeader_INCLUDED


#include "Poco/MongoDB/MongoDB.h"
#include "Poco/MongoDB/MessageHeader.h"


namespace Poco {
namespace MongoDB {


class MongoDB_API MessageHeader
	/// Represents the header which is always prepended to a request
	/// or response of MongoDB
{
public:
	static const unsigned int MSG_HEADER_SIZE = 16;

	typedef enum
	{
		  Reply = 1
		, Msg = 1000
		, Update = 2001
		, Insert = 2002
		, Query = 2004
		, GetMore = 2005
		, Delete = 2006
		, KillCursors = 2007
	} OpCode;

	virtual ~MessageHeader();
		/// Destructor

	void read(BinaryReader& reader);
		/// Reads the header

	void write(BinaryWriter& writer);
		/// Writes the header

	Int32 getMessageLength() const;
		/// Returns the message length

	OpCode opCode() const;
		/// Returns the OpCode

	Int32 getRequestID() const;
		/// Returns the request id of the current message

	void setRequestID(Int32 id);
		/// Sets the request id of the current message

	Int32 responseTo() const;
		/// Returns the request id from the original request. 

private:
	MessageHeader(OpCode opcode);
		/// Constructor.

	void setMessageLength(Int32 length);
		/// Sets the message length

	Int32 _messageLength;
	Int32 _requestID;
	Int32 _responseTo;
	OpCode _opCode;

	friend class Message;
};


inline MessageHeader::OpCode MessageHeader::opCode() const
{
	return _opCode;
}


inline Int32 MessageHeader::getMessageLength() const
{
	return _messageLength;
}


inline void MessageHeader::setMessageLength(Int32 length)
{
	poco_assert (_messageLength >= 0);
	_messageLength = MSG_HEADER_SIZE + length;
}


inline void MessageHeader::setRequestID(Int32 id)
{
	_requestID = id;
}


inline Int32 MessageHeader::getRequestID() const
{
	return _requestID;
}

inline Int32 MessageHeader::responseTo() const
{
	return _responseTo;
}


} } // namespace Poco::MongoDB


#endif //MongoDB_MessageHeader_INCLUDED
