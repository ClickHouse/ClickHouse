//
// Hash.cpp
//
// Library: Foundation
// Package: Hashing
// Module:  Hash
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Hash.h"


namespace Poco {


std::size_t hash(const std::string& str)
{
	std::size_t h = 0;
	std::string::const_iterator it  = str.begin();
	std::string::const_iterator end = str.end();
	while (it != end)
	{
		h = h * 0xf4243 ^ *it++;
	}
	return h;
}


} // namespace Poco
