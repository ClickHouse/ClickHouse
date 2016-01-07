//
// Limit.cpp
//
// $Id: //poco/Main/Data/src/Limit.cpp#5 $
//
// Library: Data
// Package: DataCore
// Module:  Limit
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/Limit.h"


namespace Poco {
namespace Data {


Limit::Limit(SizeT value, bool hardLimit, bool isLowerLimit) :
	_value(value),
	_hardLimit(hardLimit),
	_isLowerLimit(isLowerLimit)
{
}


Limit::~Limit()
{
}


} } // namespace Poco::Data
