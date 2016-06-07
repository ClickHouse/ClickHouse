//
// MessageHeader.cpp
//
// $Id$
//
// Library: MongoDB
// Package: MongoDB
// Module:  MessageHeader
//
// Implementation of the MessageHeader class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/MongoDB/Message.h"
#include "Poco/Exception.h"
#include "Poco/Net/SocketStream.h"
#include "Poco/StreamCopier.h"


namespace Poco {
namespace MongoDB {


MessageHeader::MessageHeader(OpCode opCode) : _messageLength(0), _requestID(0), _responseTo(0), _opCode(opCode)
{
}


MessageHeader::~MessageHeader()
{
}


void MessageHeader::read(BinaryReader& reader)
{
	reader >> _messageLength;
	reader >> _requestID;
	reader >> _responseTo;

	Int32 opCode;
	reader >> opCode;
	_opCode = (OpCode) opCode;

	if (!reader.good())
	{
		throw IOException("Failed to read from socket");
	}
}


void MessageHeader::write(BinaryWriter& writer)
{
	writer << _messageLength;
	writer << _requestID;
	writer << _responseTo;
	writer << (Int32) _opCode;
}


} } // namespace Poco::MongoDB
