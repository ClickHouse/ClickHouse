//
// ResponseMessage.cpp
//
// $Id$
//
// Library: MongoDB
// Package: MongoDB
// Module:  ResponseMessage
//
// Implementation of the ResponseMessage class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/MongoDB/ResponseMessage.h"
#include "Poco/Net/SocketStream.h"


namespace Poco {
namespace MongoDB {


ResponseMessage::ResponseMessage() : Message(MessageHeader::Reply), _responseFlags(0), _cursorID(0), _startingFrom(0), _numberReturned(0)
{
}


ResponseMessage::~ResponseMessage()
{
}


void ResponseMessage::clear()
{
	_responseFlags = 0;
	_startingFrom = 0;
	_cursorID = 0;
	_numberReturned = 0;
	_documents.clear();
}


void ResponseMessage::read(std::istream& istr)
{
	clear();

	BinaryReader reader(istr, BinaryReader::LITTLE_ENDIAN_BYTE_ORDER);
	
	_header.read(reader);

	reader >> _responseFlags;
	reader >> _cursorID;
	reader >> _startingFrom;
	reader >> _numberReturned;

	for(int i = 0; i < _numberReturned; ++i)
	{
		Document::Ptr doc = new Document();
		doc->read(reader);
		_documents.push_back(doc);
	}
}


} } // namespace Poco::MongoDB
