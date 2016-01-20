//
// Array.h
//
// $Id$
//
// Library: MongoDB
// Package: MongoDB
// Module:  Array
//
// Definition of the Array class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//

#ifndef MongoDB_Array_INCLUDED
#define MongoDB_Array_INCLUDED

#include "Poco/NumberFormatter.h"

#include "Poco/MongoDB/MongoDB.h"
#include "Poco/MongoDB/Document.h"

namespace Poco {
namespace MongoDB {

class MongoDB_API Array : public Document
	/// Implements the BSON Array
{
public:
	typedef SharedPtr<Array> Ptr;

	Array();
		/// Constructor

	virtual ~Array();
		/// Destructor

	template<typename T>
	T get(int pos) const
		/// Returns the element on the given index and tries to convert
		/// it to the template type. When the element is not found, a
		/// NotFoundException will be thrown. When the element can't be
		/// converted a BadCastException will be thrown.
	{
		return Document::get<T>(Poco::NumberFormatter::format(pos));
	}

	template<typename T>
	T get(int pos, const T& def) const
		/// Returns the element on the given index and tries to convert
		/// it to the template type. When the element is not found, or
		/// has the wrong type, the def argument will be returned.
	{
		return Document::get<T>(Poco::NumberFormatter::format(pos), def);
	}

	Element::Ptr get(int pos) const;
		/// Returns the element on the given index.
		/// An empty element will be returned when the element is not found.

	template<typename T>
	bool isType(int pos)
		/// Returns true when the type of the element equals the TypeId of ElementTrait
	{
		return Document::isType<T>(Poco::NumberFormatter::format(pos));
	}

	std::string toString(int indent = 0) const;
};

// BSON Embedded Array
// spec: document
template<>
struct ElementTraits<Array::Ptr>
{
	enum { TypeId = 0x04 };

	static std::string toString(const Array::Ptr& value, int indent = 0)
	{
		//TODO:
		return value.isNull() ? "null" : value->toString(indent);
	}
};


template<>
inline void BSONReader::read<Array::Ptr>(Array::Ptr& to)
{
	to->read(_reader);
}


template<>
inline void BSONWriter::write<Array::Ptr>(Array::Ptr& from)
{
	from->write(_writer);
}


} } // namespace Poco::MongoDB


#endif //MongoDB_Array_INCLUDED
