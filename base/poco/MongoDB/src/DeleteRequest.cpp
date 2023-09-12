//
// DeleteRequest.cpp
//
// Library: MongoDB
// Package: MongoDB
// Module:  DeleteRequest
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/MongoDB/DeleteRequest.h"


namespace Poco {
namespace MongoDB {


DeleteRequest::DeleteRequest(const std::string& collectionName, DeleteRequest::Flags flags):
	RequestMessage(MessageHeader::OP_DELETE), 
	_flags(flags), 
	_fullCollectionName(collectionName),
	_selector()
{
}


DeleteRequest::DeleteRequest(const std::string& collectionName, bool justOne):
	RequestMessage(MessageHeader::OP_DELETE),
	_flags(justOne ? DELETE_SINGLE_REMOVE : DELETE_DEFAULT),
	_fullCollectionName(collectionName),
	_selector()
{
}


DeleteRequest::~DeleteRequest()
{
}


void DeleteRequest::buildRequest(BinaryWriter& writer)
{
	writer << 0; // 0 - reserved for future use
	BSONWriter(writer).writeCString(_fullCollectionName);
	writer << _flags;
	_selector.write(writer);
}


} } // namespace Poco::MongoDB
