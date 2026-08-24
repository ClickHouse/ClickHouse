//
// PartSource.cpp
//
// Library: Net
// Package: Messages
// Module:  PartSource
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/PartSource.h"


namespace Poco {
namespace Net {


const int         PartSource::UNKNOWN_CONTENT_LENGTH     = -1;


PartSource::PartSource():
	_mediaType("application/octet-stream")
{
}

	
PartSource::PartSource(const std::string& mediaType):
	_mediaType(mediaType)
{
}


PartSource::~PartSource()
{
}


namespace
{
	static const std::string EMPTY;
}


const std::string& PartSource::filename() const
{
	return EMPTY;
}

std::streamsize PartSource::getContentLength() const
{
	return UNKNOWN_CONTENT_LENGTH;
}

} } // namespace Poco::Net
