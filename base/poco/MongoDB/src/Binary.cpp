//
// Binary.cpp
//
// Library: MongoDB
// Package: MongoDB
// Module:  Binary
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/MongoDB/Binary.h"


namespace Poco {
namespace MongoDB {


Binary::Binary():
	_buffer(0),
	_subtype(0)
{
}


Binary::Binary(Poco::Int32 size, unsigned char subtype):
	_buffer(size),
	_subtype(subtype)
{
}


Binary::Binary(const UUID& uuid):
	_buffer(128 / 8),
	_subtype(0x04)
{
    unsigned char szUUID[16];
    uuid.copyTo((char*) szUUID);
    _buffer.assign(szUUID, 16);
}



Binary::Binary(const std::string& data, unsigned char subtype):
	_buffer(reinterpret_cast<const unsigned char*>(data.data()), data.size()),
	_subtype(subtype)
{
}


Binary::Binary(const void* data, Poco::Int32 size, unsigned char subtype):
	_buffer(reinterpret_cast<const unsigned char*>(data), size),
	_subtype(subtype)
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
	encoder.close();
	return oss.str();
}


UUID Binary::uuid() const
{
	if ((_subtype == 0x04 || _subtype == 0x03) && _buffer.size() == 16)
	{
		UUID uuid;
		uuid.copyFrom((const char*) _buffer.begin());
		return uuid;
	}
	throw BadCastException("Invalid subtype: " + std::to_string(_subtype) + ", size: " + std::to_string(_buffer.size()));
}


} } // namespace Poco::MongoDB
