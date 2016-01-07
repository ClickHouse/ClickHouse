//
// Row.cpp
//
// $Id: //poco/Main/Data/src/Row.cpp#1 $
//
// Library: Data
// Package: DataCore
// Module:  Row
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/Row.h"
#include "Poco/Data/SimpleRowFormatter.h"
#include "Poco/String.h"
#include "Poco/Exception.h"


namespace Poco {
namespace Data {


std::ostream& operator << (std::ostream &os, const Row& row)
{
	os << row.valuesToString();
	return os;
}


Row::Row(): 
	_pNames(0),
	_pSortMap(new SortMap),
	_pFormatter(new SimpleRowFormatter)
{
}


Row::Row(NameVecPtr pNames,
	const RowFormatter::Ptr& pFormatter): _pNames(pNames)
{
	if (!_pNames) throw NullPointerException();
	init(0, pFormatter);
}


Row::Row(NameVecPtr pNames,
	const SortMapPtr& pSortMap,
	const RowFormatter::Ptr& pFormatter): _pNames(pNames)
{
	if (!_pNames) throw NullPointerException();
	init(pSortMap, pFormatter);
}


void Row::init(const SortMapPtr& pSortMap, const RowFormatter::Ptr& pFormatter)
{
	setFormatter(pFormatter);
	setSortMap(pSortMap);

	NameVec::size_type sz = _pNames->size();
	if (sz)
	{
		_values.resize(sz);
		// Row sortability in the strict weak ordering sense is 
		// an invariant, hence we must start with a zero here.
		// If null value is later retrieved from DB, the 
		// Var::empty() call should be used to empty
		// the corresponding Row value.
		_values[0] = 0;
		addSortField(0);
	}
}


Row::~Row()
{
}


Poco::Dynamic::Var& Row::get(std::size_t col)
{
	try
	{
		return _values.at(col);
	}
	catch (std::out_of_range& re)
	{
		throw RangeException(re.what());
	}
}


std::size_t Row::getPosition(const std::string& name)
{
	if (!_pNames)
		throw NullPointerException();

	NameVec::const_iterator it = _pNames->begin();
	NameVec::const_iterator end = _pNames->end();
	std::size_t col = 0;
	for (; it != end; ++it, ++col)
		if (0 == icompare(name, *it)) return col;
	
	throw NotFoundException(name);
}


void Row::checkEmpty(std::size_t pos, const Poco::Dynamic::Var& val)
{
	bool empty = true;
	SortMap::const_iterator it = _pSortMap->begin();
	SortMap::const_iterator end = _pSortMap->end();
	for (std::size_t cnt = 0; it != end; ++it, ++cnt)
	{
		if (cnt != pos)
			empty = empty && _values[it->get<0>()].isEmpty();
	}

	if (empty && val.isEmpty())
		throw IllegalStateException("All values are empty.");
}


void Row::addSortField(std::size_t pos)
{
	poco_assert (pos <= _values.size());

	checkEmpty(std::numeric_limits<std::size_t>::max(), _values[pos]);

	SortMap::iterator it = _pSortMap->begin();
	SortMap::iterator end = _pSortMap->end();
	for (; it != end; ++it)
	{
		if (it->get<0>() == pos) return; 
	}

	ComparisonType ct;
	if (_values[pos].isEmpty())
	{
		ct = COMPARE_AS_EMPTY;
	}
	else if ((_values[pos].type() == typeid(Poco::Int8))   ||
		(_values[pos].type() == typeid(Poco::UInt8))  ||
		(_values[pos].type() == typeid(Poco::Int16))  ||
		(_values[pos].type() == typeid(Poco::UInt16)) ||
		(_values[pos].type() == typeid(Poco::Int32))  ||
		(_values[pos].type() == typeid(Poco::UInt32)) ||
		(_values[pos].type() == typeid(Poco::Int64))  ||
		(_values[pos].type() == typeid(Poco::UInt64)) ||
		(_values[pos].type() == typeid(bool)))
	{
		ct = COMPARE_AS_INTEGER;
	}
	else if ((_values[pos].type() == typeid(float)) ||
		(_values[pos].type() == typeid(double)))
	{
		ct = COMPARE_AS_FLOAT;
	}
	else
	{
		ct = COMPARE_AS_STRING;
	}

	_pSortMap->push_back(SortTuple(pos, ct));
}


void Row::addSortField(const std::string& name)
{
	addSortField(getPosition(name));
}


void Row::removeSortField(std::size_t pos)
{
	checkEmpty(pos, Poco::Dynamic::Var());

	SortMap::iterator it = _pSortMap->begin();
	SortMap::iterator end = _pSortMap->end();
	for (; it != end; ++it)
	{
		if (it->get<0>() == pos)
		{
			_pSortMap->erase(it);
			return;
		}
	}
}


void Row::removeSortField(const std::string& name)
{
	removeSortField(getPosition(name));
}


void Row::replaceSortField(std::size_t oldPos, std::size_t newPos)
{
	poco_assert (oldPos <= _values.size());
	poco_assert (newPos <= _values.size());

	ComparisonType ct;

	if (_values[newPos].isEmpty())
	{
		ct = COMPARE_AS_EMPTY;
	}
	else if ((_values[newPos].type() == typeid(Poco::Int8)) ||
		(_values[newPos].type() == typeid(Poco::UInt8))     ||
		(_values[newPos].type() == typeid(Poco::Int16))     ||
		(_values[newPos].type() == typeid(Poco::UInt16))    ||
		(_values[newPos].type() == typeid(Poco::Int32))     ||
		(_values[newPos].type() == typeid(Poco::UInt32))    ||
		(_values[newPos].type() == typeid(Poco::Int64))     ||
		(_values[newPos].type() == typeid(Poco::UInt64))    ||
		(_values[newPos].type() == typeid(bool)))
	{
		ct = COMPARE_AS_INTEGER;
	}
	else if ((_values[newPos].type() == typeid(float)) ||
		(_values[newPos].type() == typeid(double)))
	{
		ct = COMPARE_AS_FLOAT;
	}
	else
	{
		ct = COMPARE_AS_STRING;
	}

	SortMap::iterator it = _pSortMap->begin();
	SortMap::iterator end = _pSortMap->end();
	for (; it != end; ++it)
	{
		if (it->get<0>() == oldPos)
		{
			*it = SortTuple(newPos, ct);
			return;
		}
	}

	throw NotFoundException("Field not found");
}


void Row::replaceSortField(const std::string& oldName, const std::string& newName)
{
	replaceSortField(getPosition(oldName), getPosition(newName));
}


void Row::resetSort()
{
	_pSortMap->clear();
	if (_values.size())	addSortField(0);
}


bool Row::isEqualSize(const Row& other) const
{
	return (other._values.size() == _values.size());
}


bool Row::isEqualType(const Row& other) const
{
	std::vector<Poco::Dynamic::Var>::const_iterator it = _values.begin();
	std::vector<Poco::Dynamic::Var>::const_iterator end = _values.end();
	for (int i = 0; it != end; ++it, ++i)
	{
		if (it->type() != other._values[i].type())
			return false;
	}

	return true;
}


bool Row::operator == (const Row& other) const
{
	if (!isEqualSize(other)) return false;
	if (!isEqualType(other)) return false;

	std::vector<Poco::Dynamic::Var>::const_iterator it = _values.begin();
	std::vector<Poco::Dynamic::Var>::const_iterator end = _values.end();
	for (int i = 0; it != end; ++it, ++i)
	{
		if ((*it).convert<std::string>() != other._values[i].convert<std::string>())
			return false;
	}

	return true;
}


bool Row::operator != (const Row& other) const
{
	return !(*this == other);
}


bool Row::operator < (const Row& other) const
{
	if (*_pSortMap != *other._pSortMap)
		throw InvalidAccessException("Rows compared have different sorting criteria.");

	SortMap::const_iterator it = _pSortMap->begin();
	SortMap::const_iterator end = _pSortMap->end();
	for (; it != end; ++it)
	{
		switch (it->get<1>())
		{
		case COMPARE_AS_EMPTY:
			return false;

		case COMPARE_AS_INTEGER:
			if (_values[it->get<0>()].convert<Poco::Int64>() < 
				other._values[it->get<0>()].convert<Poco::Int64>())
				return true;
			else if (_values[it->get<0>()].convert<Poco::Int64>() != 
				other._values[it->get<0>()].convert<Poco::Int64>())
				return false;
			break;

		case COMPARE_AS_FLOAT:
			if (_values[it->get<0>()].convert<double>() < 
				other._values[it->get<0>()].convert<double>())
				return true;
			else if (_values[it->get<0>()].convert<double>() !=
				other._values[it->get<0>()].convert<double>())
				return false;
			break;

		case COMPARE_AS_STRING:
			if (_values[it->get<0>()].convert<std::string>() < 
				other._values[it->get<0>()].convert<std::string>())
				return true;
			else if (_values[it->get<0>()].convert<std::string>() !=
				other._values[it->get<0>()].convert<std::string>())
				return false;
			break;

		default:
			throw IllegalStateException("Unknown comparison criteria.");
		}
	}

	return false;
}


void Row::setFormatter(const RowFormatter::Ptr& pFormatter)
{
	if (pFormatter.get())
		_pFormatter = pFormatter;
	else 
		_pFormatter = new SimpleRowFormatter;
}


void Row::setSortMap(const SortMapPtr& pSortMap)
{
	if (pSortMap.get())
		_pSortMap = pSortMap;
	else 
		_pSortMap = new SortMap;
}


const std::string& Row::namesToString() const
{
	if (!_pNames)
		throw NullPointerException();

	return _pFormatter->formatNames(names(), _nameStr);
}


void Row::formatNames() const
{
	if (!_pNames)
		throw NullPointerException();

	return _pFormatter->formatNames(names());
}


} } // namespace Poco::Data
