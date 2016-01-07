//
// RowFilter.cpp
//
// $Id: //poco/Main/Data/src/RowFilter.cpp#1 $
//
// Library: Data
// Package: DataCore
// Module:  RowFilter
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/RowFilter.h"
#include "Poco/Data/RecordSet.h"
#include "Poco/String.h"
#include "Poco/Exception.h"
#include <functional>

namespace Poco {
namespace Data {


RowFilter::RowFilter(RecordSet* pRecordSet): _pRecordSet(pRecordSet), _not(false)
{
	poco_check_ptr(pRecordSet);
	init();
	_pRecordSet->filter(this);
}


RowFilter::RowFilter(Ptr pParent, LogicOperator op): _pRecordSet(0),
	_pParent(pParent),
	_not(false)
{
	poco_check_ptr(_pParent.get());
	init();
	_pParent->addFilter(this, op);
}


void RowFilter::init()
{
	_comparisons.insert(Comparisons::value_type("<", VALUE_LESS_THAN));
	_comparisons.insert(Comparisons::value_type("<=", VALUE_LESS_THAN_OR_EQUAL));
	_comparisons.insert(Comparisons::value_type("=", VALUE_EQUAL));
	_comparisons.insert(Comparisons::value_type("==", VALUE_EQUAL));
	_comparisons.insert(Comparisons::value_type(">", VALUE_GREATER_THAN));
	_comparisons.insert(Comparisons::value_type(">=", VALUE_GREATER_THAN_OR_EQUAL));
	_comparisons.insert(Comparisons::value_type("<>", VALUE_NOT_EQUAL));
	_comparisons.insert(Comparisons::value_type("!=", VALUE_NOT_EQUAL));
	_comparisons.insert(Comparisons::value_type("IS NULL", VALUE_IS_NULL));

	duplicate();
}


RowFilter::~RowFilter()
{
	try
	{
		release();
		if (_pRecordSet) _pRecordSet->filter(0);
		if (_pParent.get()) _pParent->removeFilter(this);
	}
	catch (...)
	{
		poco_unexpected();
	}
}


bool RowFilter::isAllowed(std::size_t row) const
{
	Poco::Dynamic::Var retVal;
	const RecordSet& rs = recordSet();
	
	std::size_t columns = rs.columnCount();
	ComparisonMap::const_iterator it = _comparisonMap.begin();
	ComparisonMap::const_iterator end = _comparisonMap.end();
	for (; it != end; ++it)
	{
		for (std::size_t col = 0; col < columns; ++col)
		{
			const std::string name = toUpper(rs.metaColumn(static_cast<UInt32>(col)).name());
			if (_comparisonMap.find(name) == _comparisonMap.end()) continue;

			Poco::Dynamic::Var ret;
			CompT compOp = 0;
			Poco::Dynamic::Var val = rs.value(col, row, false);

			switch (it->second.get<1>())
			{
			case VALUE_LESS_THAN:
				compOp = less; break;
			case VALUE_LESS_THAN_OR_EQUAL:
				compOp = lessOrEqual; break;
			case VALUE_EQUAL:
				compOp = equal; break;
			case VALUE_GREATER_THAN:
				compOp = greater; break;
			case VALUE_GREATER_THAN_OR_EQUAL:
				compOp = greaterOrEqual; break;
			case VALUE_NOT_EQUAL:
				compOp = notEqual; break;
			case VALUE_IS_NULL:
				compOp = isNull; break;
			default:
				throw IllegalStateException("Unsupported comparison criteria.");
			}
			
			doCompare(ret, val, compOp, it->second);
			if (retVal.isEmpty()) retVal = ret;
			else retVal = retVal || ret;
		}
	}

	// iterate through children
	FilterMap::const_iterator fIt = _filterMap.begin();
	FilterMap::const_iterator fEnd = _filterMap.end();
	for (; fIt != fEnd; ++fIt)
	{
		if (OP_OR == fIt->second)
		{
			if (retVal.isEmpty())
				retVal = fIt->first->isAllowed(row);
			else
				retVal = retVal || fIt->first->isAllowed(row);
		}
		else if (OP_AND == fIt->second)
		{
			if (retVal.isEmpty())
				retVal = fIt->first->isAllowed(row);
			else
				retVal = retVal && fIt->first->isAllowed(row);
		}
		else
			throw IllegalStateException("Unknown logical operation.");
	}

	if (retVal.isEmpty()) retVal = true; // no filtering found
	return (!_not) && retVal.extract<bool>();
}


int RowFilter::remove(const std::string& name)
{
	poco_check_ptr (_pRecordSet);
	_pRecordSet->moveFirst();
	return static_cast<int>(_comparisonMap.erase(toUpper(name)));
}


RowFilter::Comparison RowFilter::getComparison(const std::string& comp) const
{
	Comparisons::const_iterator it = _comparisons.find(toUpper(comp));
	if (it == _comparisons.end())
		throw NotFoundException("Comparison not found", comp);

	return it->second;
}


void RowFilter::addFilter(const Ptr& pFilter, LogicOperator comparison)
{
	poco_check_ptr (_pRecordSet);

	pFilter->_pRecordSet = _pRecordSet;
	_pRecordSet->moveFirst();
	_filterMap.insert(FilterMap::value_type(pFilter, comparison));
}


void RowFilter::removeFilter(const Ptr& pFilter)
{
	poco_check_ptr (_pRecordSet);

	pFilter->_pRecordSet = 0;
	_pRecordSet->moveFirst();
	_filterMap.erase(pFilter);
}


void RowFilter::doCompare(Poco::Dynamic::Var& ret,
	Poco::Dynamic::Var& val,
	CompT comp,
	const ComparisonEntry& ce)
{
	if (ret.isEmpty()) ret = comp(val, ce.get<0>());
	else
	{
		if (ce.get<2>() == OP_OR)
			ret = ret || comp(val, ce.get<0>());
		else if (ce.get<2>() == OP_AND)
			ret = ret && comp(val, ce.get<0>());
		else
			throw IllegalStateException("Unknown logical operation.");
	}
}

RecordSet& RowFilter::recordSet() const
{
	if (!_pRecordSet)
	{
		Ptr pParent = _pParent;
		while (pParent && !_pRecordSet)
			_pRecordSet = pParent->_pRecordSet;
	}
	poco_check_ptr (_pRecordSet);
	return *_pRecordSet; 
}


} } // namespace Poco::Data
