//
// InsertRequest.cpp
//
// Library: MongoDB
// Package: MongoDB
// Module:  InsertRequest
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/MongoDB/InsertRequest.h"


namespace Poco {
namespace MongoDB {


InsertRequest::InsertRequest(const std::string& collectionName, Flags flags):
	RequestMessage(MessageHeader::OP_INSERT), 
	_flags(flags),
	_fullCollectionName(collectionName)
{
}


InsertRequest::~InsertRequest()
{
}


void InsertRequest::buildRequest(BinaryWriter& writer)
{
	poco_assert (!_documents.empty());

	writer << _flags;
	BSONWriter bsonWriter(writer);
	bsonWriter.writeCString(_fullCollectionName);
	for (Document::Vector::iterator it = _documents.begin(); it != _documents.end(); ++it)
	{
		bsonWriter.write(*it);
	}
}


} } // namespace Poco::MongoDB
