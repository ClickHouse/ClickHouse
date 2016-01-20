//
// ObjectId.cpp
//
// $Id$
//
// Library: MongoDB
// Package: MongoDB
// Module:  ObjectId
//
// Implementation of the ObjectId class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//

#include "Poco/MongoDB/ObjectId.h"
#include "Poco/Format.h"


namespace Poco {
namespace MongoDB {

ObjectId::ObjectId()
{
	memset(_id, 0, sizeof(_id));
}

ObjectId::ObjectId(const std::string& id)
{
	poco_assert_dbg(id.size() == 24);

    const char *p = id.c_str();
    for (std::size_t i = 0; i < 12; ++i) {
		_id[i] = fromHex(p);
		p += 2;
	}
}

ObjectId::ObjectId(const ObjectId& copy)
{
	memcpy(_id, copy._id, sizeof(_id));
}

ObjectId::~ObjectId()
{
}


std::string ObjectId::toString(const std::string& fmt) const
{
	std::string s;

	for(int i = 0; i < 12; ++i)
	{
		s += format(fmt, (unsigned int) _id[i]);
	}
	return s;
}


} } // namespace Poco::MongoDB
