//
// QueryRequest.cpp
//
// $Id$
//
// Library: MongoDB
// Package: MongoDB
// Module:  QueryRequest
//
// Implementation of the QueryRequest class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/MongoDB/QueryRequest.h"


namespace Poco {
namespace MongoDB {


QueryRequest::QueryRequest(const std::string& collectionName, QueryRequest::Flags flags) 
	: RequestMessage(MessageHeader::Query), 
	_flags(flags), 
	_fullCollectionName(collectionName),
	_numberToSkip(0), 
	_numberToReturn(100),
	_selector(),
	_returnFieldSelector()
{
}


QueryRequest::~QueryRequest()
{
}


void QueryRequest::buildRequest(BinaryWriter& writer)
{
	writer << _flags;
	BSONWriter(writer).writeCString(_fullCollectionName);
	writer << _numberToSkip;
	writer << _numberToReturn;
	_selector.write(writer);

	if ( ! _returnFieldSelector.empty() )
	{
		_returnFieldSelector.write(writer);
	}
}


} } // namespace Poco::MongoDB
