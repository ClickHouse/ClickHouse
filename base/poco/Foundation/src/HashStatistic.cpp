//
// HashStatistic.cpp
//
// Library: Foundation
// Package: Hashing
// Module:  HashStatistic
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/HashStatistic.h"
#include <sstream>

namespace Poco {


HashStatistic::HashStatistic(
	UInt32 tableSize, 
	UInt32 numEntries, 
	UInt32 numZeroEntries, 
	UInt32 maxEntry, 
	std::vector<UInt32> details):
	_sizeOfTable(tableSize),
	_numberOfEntries(numEntries),
	_numZeroEntries(numZeroEntries),
	_maxEntriesPerHash(maxEntry),
	_detailedEntriesPerHash(details)
{
}


HashStatistic::~HashStatistic()
{
}


std::string HashStatistic::toString() const
{
	std::ostringstream str;
	str << "HashTable of size " << _sizeOfTable << " containing " << _numberOfEntries << " entries:\n";
	str << "  NumberOfZeroEntries: " << _numZeroEntries << "\n";
	str << "  MaxEntry: " << _maxEntriesPerHash << "\n";
	str << "  AvgEntry: " << avgEntriesPerHash() << ", excl Zero slots: " << avgEntriesPerHashExclZeroEntries() << "\n";
	str << "  DetailedStatistics: \n";
	for (int i = 0; i < _detailedEntriesPerHash.size(); ++i)
	{
		// 10 entries per line
		if (i % 10 == 0)
		{
			str << "\n  " << i << ":";
		}
		str << " " << _detailedEntriesPerHash[i];
	}
	str << "\n";
	str.flush();
	return str.str();
}


} // namespace Poco
