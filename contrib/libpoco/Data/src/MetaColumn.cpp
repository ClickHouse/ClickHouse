//
// MetaColumn.cpp
//
// $Id: //poco/Main/Data/src/MetaColumn.cpp#2 $
//
// Library: Data
// Package: DataCore
// Module:  MetaColumn
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/MetaColumn.h"


namespace Poco {
namespace Data {


MetaColumn::MetaColumn()
{
}


MetaColumn::MetaColumn(std::size_t position,
	const std::string& name,
	ColumnDataType type,
	std::size_t length,
	std::size_t precision,
	bool nullable): 
	_name(name),
	_length(length),
	_precision(precision),
	_position(position),
	_type(type),
	_nullable(nullable)
{
}


MetaColumn::~MetaColumn()
{
}


} } // namespace Poco::Data
