//
// AbstractExtraction.cpp
//
// $Id: //poco/Main/Data/src/AbstractExtraction.cpp#2 $
//
// Library: Data
// Package: DataCore
// Module:  AbstractExtraction
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/AbstractExtraction.h"


namespace Poco {
namespace Data {


AbstractExtraction::AbstractExtraction(Poco::UInt32 limit,
	Poco::UInt32 position,
	bool bulk): 
	_pExtractor(0), 
	_limit(limit),
	_position(position),
	_bulk(bulk),
	_emptyStringIsNull(false),
	_forceEmptyString(false)
{
}


AbstractExtraction::~AbstractExtraction()
{
}


} } // namespace Poco::Data
