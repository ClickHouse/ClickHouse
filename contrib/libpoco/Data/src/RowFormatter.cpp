//
// RowFormatter.cpp
//
// $Id: //poco/Main/Data/src/RowFormatter.cpp#1 $
//
// Library: Data
// Package: DataCore
// Module:  RowFormatter
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/RowFormatter.h"
#include "Poco/Exception.h"
#include <iomanip>


namespace Poco {
namespace Data {


RowFormatter::RowFormatter(const std::string& prefix,
	const std::string& postfix,
	Mode mode):
	_prefix(prefix), 
	_postfix(postfix),
	_mode(mode),
	_totalRowCount(0)
{
}


RowFormatter::~RowFormatter()
{
}


std::string& RowFormatter::formatNames(const NameVecPtr pNames, std::string& formattedNames)
{
	formattedNames.clear();
	return formattedNames;
}


void RowFormatter::formatNames(const NameVecPtr pNames)
{
	return;
}


std::string& RowFormatter::formatValues(const ValueVec& vals, std::string& formattedValues)
{
	formattedValues.clear();
	return formattedValues;
}


void RowFormatter::formatValues(const ValueVec& vals)
{
	return;
}


const std::string& RowFormatter::toString()
{
	throw NotImplementedException("RowFormatter::toString()");
}


void RowFormatter::reset()
{
	_prefix = "";
	_postfix = "";
	_totalRowCount = INVALID_ROW_COUNT;
}


} } // namespace Poco::Data
