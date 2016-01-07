//
// InsertRequest.cpp
//
// $Id$
//
// Library: MongoDB
// Package: MongoDB
// Module:  InsertRequest
//
// Implementation of the InsertRequest class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/MongoDB/InsertRequest.h"


namespace Poco {
namespace MongoDB {


InsertRequest::InsertRequest(const std::string& collectionName, Flags flags) 
	: RequestMessage(MessageHeader::Insert), 
	_flags(flags),
	_fullCollectionName(collectionName)
{
}


InsertRequest::~InsertRequest()
{
}


void InsertRequest::buildRequest(BinaryWriter& writer)
{
	//TODO: throw exception when no document is added

	writer << _flags;
	BSONWriter bsonWriter(writer);
	bsonWriter.writeCString(_fullCollectionName);
	for(Document::Vector::iterator it = _documents.begin(); it != _documents.end(); ++it)
	{
		bsonWriter.write(*it);
	}
}


} } // namespace Poco::MongoDB
