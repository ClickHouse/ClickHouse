//
// Binary.cpp
//
// $Id$
//
// Library: MongoDB
// Package: MongoDB
// Module:  Binary
//
// Implementation of the Binary class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/MongoDB/Binary.h"


namespace Poco {
namespace MongoDB {


Binary::Binary() : _buffer(0)
{
}


Binary::Binary(Poco::Int32 size, unsigned char subtype) : _buffer(size), _subtype(subtype)
{
}


Binary::~Binary()
{
}


std::string Binary::toString(int indent) const
{
	std::ostringstream oss;
	Base64Encoder encoder(oss);
	MemoryInputStream mis((const char*) _buffer.begin(), _buffer.size());
	StreamCopier::copyStream(mis, encoder);
	return oss.str();
}


} } // namespace Poco::MongoDB
