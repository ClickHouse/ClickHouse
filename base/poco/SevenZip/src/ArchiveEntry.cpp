//
// ArchiveEntry.cpp
//
// Library: SevenZip
// Package: Archive
// Module:  ArchiveEntry
//
// Copyright (c) 2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/SevenZip/ArchiveEntry.h"
#include <algorithm>


namespace Poco {
namespace SevenZip {


ArchiveEntry::ArchiveEntry():
	_type(ENTRY_FILE),
	_size(0),
	_lastModified(0),
	_attributes(0),
	_index(0)
{
}


ArchiveEntry::ArchiveEntry(EntryType type, const std::string& path, Poco::UInt64 size, Poco::Timestamp lastModified, UInt32 attributes, Poco::UInt32 index):
	_type(type),
	_path(path),
	_size(size),
	_lastModified(lastModified),
	_attributes(attributes),
	_index(index)
{
}


ArchiveEntry::ArchiveEntry(const ArchiveEntry& entry):
	_type(entry._type),
	_path(entry._path),
	_size(entry._size),
	_lastModified(entry._lastModified),
	_attributes(entry._attributes),
	_index(entry._index)
{
}

	
ArchiveEntry::~ArchiveEntry()
{
}

	
ArchiveEntry& ArchiveEntry::operator = (const ArchiveEntry& entry)
{
	ArchiveEntry temp(entry);
	swap(temp);
	return *this;
}


void ArchiveEntry::swap(ArchiveEntry& entry)
{
	std::swap(_type, entry._type);
	std::swap(_path, entry._path);
	std::swap(_size, entry._size);
	_lastModified.swap(entry._lastModified);
	std::swap(_attributes, entry._attributes);
	std::swap(_index, entry._index);
}


} } // namespace Poco::SevenZip
