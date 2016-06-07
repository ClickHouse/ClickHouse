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


#include "Poco/Data/SimpleRowFormatter.h"
#include "Poco/Exception.h"
#include <iomanip>


namespace Poco {
namespace Data {


SimpleRowFormatter::SimpleRowFormatter(std::streamsize columnWidth, std::streamsize spacing):
	_colWidth(columnWidth), _spacing(spacing), _rowCount(0)
{
}


SimpleRowFormatter::SimpleRowFormatter(const SimpleRowFormatter& other):
	RowFormatter(other.prefix(), other.postfix()),
	_colWidth(other._colWidth),
	_spacing(other._spacing),
	_rowCount(0)
{
}


SimpleRowFormatter::~SimpleRowFormatter()
{
}


SimpleRowFormatter& SimpleRowFormatter::operator = (const SimpleRowFormatter& row)
{
	SimpleRowFormatter tmp(row);
	swap(tmp);
	return *this;
}


void SimpleRowFormatter::swap(SimpleRowFormatter& other)
{
	using std::swap;

	setPrefix(other.prefix());
	setPostfix(other.postfix());
	swap(_colWidth, other._colWidth);
	swap(_spacing, other._spacing);
}


std::string& SimpleRowFormatter::formatNames(const NameVecPtr pNames, std::string& formattedNames)
{
	_rowCount = 0;

	std::ostringstream str;
	std::string line(std::string::size_type(pNames->size()*_colWidth + (pNames->size() - 1)*_spacing), '-');
	std::string space(_spacing, ' ');
	NameVec::const_iterator it = pNames->begin();
	NameVec::const_iterator end = pNames->end();
	for (; it != end; ++it)
	{
		if (it != pNames->begin()) str << space;
		str << std::left << std::setw(_colWidth) << *it;
	}
	str << std::endl << line << std::endl;

	return formattedNames = str.str();
}


std::string& SimpleRowFormatter::formatValues(const ValueVec& vals, std::string& formattedValues)
{
	std::ostringstream str;
	std::string space(_spacing, ' ');
	ValueVec::const_iterator it = vals.begin();
	ValueVec::const_iterator end = vals.end();
	for (; it != end; ++it)
	{
		if (it != vals.begin()) str << space;
		if (it->isNumeric())
		{
			str << std::right 
				<< std::fixed
				<< std::setprecision(2);
		}
		else str << std::left;

		if (!it->isEmpty())
			str << std::setw(_colWidth) << it->convert<std::string>();
		else
			str << std::setw(_colWidth) << "null";
	}
	str << std::endl;

	++_rowCount;

	return formattedValues = str.str();
}


} } // namespace Poco::Data
