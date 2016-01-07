//
// KillCursorsRequest.cpp
//
// $Id$
//
// Library: MongoDB
// Package: MongoDB
// Module:  KillCursorsRequest
//
// Implementation of the KillCursorsRequest class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/MongoDB/KillCursorsRequest.h"


namespace Poco {
namespace MongoDB {


KillCursorsRequest::KillCursorsRequest() 
	: RequestMessage(MessageHeader::KillCursors)
{
}


KillCursorsRequest::~KillCursorsRequest()
{
}


void KillCursorsRequest::buildRequest(BinaryWriter& writer)
{
	writer << 0; // 0 - reserved for future use
	writer << _cursors.size();
	for(std::vector<Int64>::iterator it = _cursors.begin(); it != _cursors.end(); ++it)
	{
		writer << *it;
	}		
}


} } // namespace Poco::MongoDB
