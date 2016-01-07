//
// Bulk.cpp
//
// $Id: //poco/Main/Data/src/Bulk.cpp#7 $
//
// Library: Data
// Package: DataCore
// Module:  Bulk
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/Bulk.h"


namespace Poco {
namespace Data {


Bulk::Bulk(const Limit& limit): _limit(limit.value(), false, false)
{
}


Bulk::Bulk(Poco::UInt32 value): _limit(value, false, false)
{
}


Bulk::~Bulk()
{
}


} } // namespace Poco::Data
