//
// DoubleByteEncoding.cpp
//
// Library: Encodings
// Package: Encodings
// Module:  DoubleByteEncoding
//
// Copyright (c) 2018, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


// Workaround for an issue with the Visual C++ 2008 STL
// implementation. If iterator debugging is enabled (default
// if _DEBUG is #define'd), std::lower_bound() will fail
// to compile.
// See also: <https://github.com/pocoproject/poco/issues/2220>
#if defined(_MSC_VER) && defined(_DEBUG) && _MSC_VER <= 1500
#define _HAS_ITERATOR_DEBUGGING 0
#endif


#include "Poco/DoubleByteEncoding.h"
#include "Poco/String.h"
#include <algorithm>


namespace Poco {


DoubleByteEncoding::DoubleByteEncoding(const char* names[], const TextEncoding::CharacterMap& charMap, const Mapping mappingTable[], std::size_t mappingTableSize, const Mapping reverseMappingTable[], std::size_t reverseMappingTableSize):
	_names(names),
	_charMap(charMap),
	_mappingTable(mappingTable),
	_mappingTableSize(mappingTableSize),
	_reverseMappingTable(reverseMappingTable),
	_reverseMappingTableSize(reverseMappingTableSize)
{
}


DoubleByteEncoding::~DoubleByteEncoding()
{
}


const char* DoubleByteEncoding::canonicalName() const
{
	return _names[0];
}


bool DoubleByteEncoding::isA(const std::string& encodingName) const
{
	for (const char** name = _names; *name; ++name)
	{
		if (Poco::icompare(encodingName, *name) == 0)
			return true;
	}
	return false;
}


const TextEncoding::CharacterMap& DoubleByteEncoding::characterMap() const
{
	return _charMap;
}


int DoubleByteEncoding::convert(const unsigned char* bytes) const
{
	int n = _charMap[*bytes];
	switch (n)
	{
	case -1:
		return -1;
	case -2:
		return map(static_cast<Poco::UInt16>(bytes[0] << 8) | bytes[1]);
	default:
		return n;
	}
}


int DoubleByteEncoding::convert(int ch, unsigned char* bytes, int length) const
{
	int n = reverseMap(ch);
	if (n < 0) return 0;
	if (!bytes || !length)
	{
		return n > 0xFF ? 2 : 1;
	}
	if (n > 0xFF && length < 2) return 0;

	if (n > 0xFF)
	{
		bytes[0] = static_cast<unsigned char>(n >> 8);
		bytes[1] = static_cast<unsigned char>(n & 0xFF);
		return 2;
	}
	else
	{
		bytes[0] = static_cast<unsigned char>(n);
		return 1;
	}
}


int DoubleByteEncoding::queryConvert(const unsigned char* bytes, int length) const
{
	int n = _charMap[*bytes];
	switch (n)
	{
	case -1:
		return -1;
	case -2:
		if (length >= 2)
			return map((bytes[0] << 8) | bytes[1]);
		else
			return -2;
	default:
		return n;
	}
}


int DoubleByteEncoding::sequenceLength(const unsigned char* bytes, int length) const
{
	if (1 <= length)
	{
		int cc = _charMap[*bytes];
		if (cc >= 0)
			return 1;
		else if (cc < -1)
			return -cc;
		else
			return -1;
	}
	else return -1;
}


struct MappingLessThan
{
	bool operator () (const DoubleByteEncoding::Mapping& mapping, const Poco::UInt16& key) const
	{
		return mapping.from < key;
	}
};


int DoubleByteEncoding::map(Poco::UInt16 encoded) const
{
	const Mapping* begin = _mappingTable;
	const Mapping* end = begin + _mappingTableSize;
	const Mapping* it = std::lower_bound(begin, end, encoded, MappingLessThan());
	if (it != end && it->from == encoded)
		return it->to;
	else
		return -1;
}


int DoubleByteEncoding::reverseMap(int cp) const
{
	const Mapping* begin = _reverseMappingTable;
	const Mapping* end = begin + _reverseMappingTableSize;
	const Mapping* it = std::lower_bound(begin, end, static_cast<Poco::UInt16>(cp), MappingLessThan());
	if (it != end && it->from == cp)
		return it->to;
	else
		return -1;
}


} // namespace Poco
