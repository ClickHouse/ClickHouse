//
// RequestMessage.h
//
// Library: MongoDB
// Package: MongoDB
// Module:  RequestMessage
//
// Definition of the RequestMessage class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MongoDB_RequestMessage_INCLUDED
#define MongoDB_RequestMessage_INCLUDED


#include "Poco/MongoDB/MongoDB.h"
#include "Poco/MongoDB/Message.h"
#include <ostream>


namespace Poco {
namespace MongoDB {


class MongoDB_API RequestMessage: public Message
	/// Base class for a request sent to the MongoDB server.
{
public:
	explicit RequestMessage(MessageHeader::OpCode opcode);
		/// Creates a RequestMessage using the given opcode.

	virtual ~RequestMessage();
		/// Destroys the RequestMessage.

	void send(std::ostream& ostr);
		/// Writes the request to stream.

protected:
	virtual void buildRequest(BinaryWriter& ss) = 0;
};


} } // namespace Poco::MongoDB


#endif // MongoDB_RequestMessage_INCLUDED
