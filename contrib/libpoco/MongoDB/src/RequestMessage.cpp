//
// RequestMessage.cpp
//
// $Id$
//
// Library: MongoDB
// Package: MongoDB
// Module:  RequestMessage
//
// Implementation of the RequestMessage class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/MongoDB/RequestMessage.h"
#include "Poco/Net/SocketStream.h"
#include "Poco/StreamCopier.h"


namespace Poco {
namespace MongoDB {


RequestMessage::RequestMessage(MessageHeader::OpCode opcode) : Message(opcode)
{
}


RequestMessage::~RequestMessage()
{
}


void RequestMessage::send(std::ostream& ostr)
{
	std::stringstream ss;
	BinaryWriter requestWriter(ss);
	buildRequest(requestWriter);
	requestWriter.flush();

	messageLength(static_cast<Poco::Int32>(ss.tellp()));

	BinaryWriter socketWriter(ostr, BinaryWriter::LITTLE_ENDIAN_BYTE_ORDER);
	_header.write(socketWriter);
	StreamCopier::copyStream(ss, ostr);
	ostr.flush();
}


} } // namespace Poco::MongoDB
