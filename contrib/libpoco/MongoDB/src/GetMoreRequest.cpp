//
// GetMoreRequest.cpp
//
// $Id$
//
// Library: MongoDB
// Package: MongoDB
// Module:  GetMoreRequest
//
// Implementation of the GetMoreRequest class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/MongoDB/GetMoreRequest.h"
#include "Poco/MongoDB/Element.h"


namespace Poco {
namespace MongoDB {


GetMoreRequest::GetMoreRequest(const std::string& collectionName, Int64 cursorID) 
	: RequestMessage(MessageHeader::GetMore), 
	_fullCollectionName(collectionName),
	_numberToReturn(100),
	_cursorID(cursorID)
{
}


GetMoreRequest::~GetMoreRequest()
{
}


void GetMoreRequest::buildRequest(BinaryWriter& writer)
{
	writer << 0; // 0 - reserved for future use
	BSONWriter(writer).writeCString(_fullCollectionName);
	writer << _numberToReturn;
	writer << _cursorID;
}


} } // namespace Poco::MongoDB
