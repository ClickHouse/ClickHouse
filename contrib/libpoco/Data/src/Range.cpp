//
// Range.cpp
//
// $Id: //poco/Main/Data/src/Range.cpp#2 $
//
// Library: Data
// Package: DataCore
// Module:  Range
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/Range.h"
#include "Poco/Data/DataException.h"


namespace Poco {
namespace Data {


using namespace Keywords;


Range::Range(Limit::SizeT lowValue, Limit::SizeT upValue, bool hardLimit) :
	_lower(lowerLimit(lowValue)),
	_upper(upperLimit(upValue, hardLimit))
{
	if (lowValue > upValue)
		throw LimitException("lowerLimit > upperLimit!");
}


Range::~Range()
{
}


} } // namespace Poco::Data
