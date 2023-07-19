//
// KillCursorsRequest.cpp
//
// Library: MongoDB
// Package: MongoDB
// Module:  KillCursorsRequest
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/MongoDB/KillCursorsRequest.h"


namespace Poco {
namespace MongoDB {


KillCursorsRequest::KillCursorsRequest():
	RequestMessage(MessageHeader::OP_KILL_CURSORS)
{
}


KillCursorsRequest::~KillCursorsRequest()
{
}


void KillCursorsRequest::buildRequest(BinaryWriter& writer)
{
	writer << 0; // 0 - reserved for future use
	writer << static_cast<Poco::UInt64>(_cursors.size());
	for (std::vector<Int64>::iterator it = _cursors.begin(); it != _cursors.end(); ++it)
	{
		writer << *it;
	}
}


} } // namespace Poco::MongoDB
