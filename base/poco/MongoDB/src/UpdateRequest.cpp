//
// UpdateRequest.cpp
//
// Library: MongoDB
// Package: MongoDB
// Module:  UpdateRequest
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/MongoDB/UpdateRequest.h"


namespace Poco {
namespace MongoDB {


UpdateRequest::UpdateRequest(const std::string& collectionName, UpdateRequest::Flags flags):
	RequestMessage(MessageHeader::OP_UPDATE),
	_flags(flags),
	_fullCollectionName(collectionName),
	_selector(),
	_update()
{
}


UpdateRequest::~UpdateRequest()
{
}


void UpdateRequest::buildRequest(BinaryWriter& writer)
{
	writer << 0; // 0 - reserved for future use
	BSONWriter(writer).writeCString(_fullCollectionName);
	writer << _flags;
	_selector.write(writer);
	_update.write(writer);
}


} } // namespace Poco::MongoDB
