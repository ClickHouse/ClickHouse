//
// Hash.h
//
// $Id: //poco/1.4/Foundation/include/Poco/Hash.h#1 $
//
// Library: Foundation
// Package: Hashing
// Module:  Hash
//
// Definition of the Hash class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Hash_INCLUDED
#define Foundation_Hash_INCLUDED


#include "Poco/Foundation.h"
#include <cstddef>


namespace Poco {


std::size_t Foundation_API hash(Int8 n);
std::size_t Foundation_API hash(UInt8 n);
std::size_t Foundation_API hash(Int16 n);
std::size_t Foundation_API hash(UInt16 n);
std::size_t Foundation_API hash(Int32 n);
std::size_t Foundation_API hash(UInt32 n);
std::size_t Foundation_API hash(Int64 n);
std::size_t Foundation_API hash(UInt64 n);
std::size_t Foundation_API hash(const std::string& str);


template <class T>
struct Hash
	/// A generic hash function.
{
	std::size_t operator () (T value) const
		/// Returns the hash for the given value.
	{
		return Poco::hash(value);
	}
};


//
// inlines
//
inline std::size_t hash(Int8 n)
{
	return static_cast<std::size_t>(n)*2654435761U; 
}


inline std::size_t hash(UInt8 n)
{
	return static_cast<std::size_t>(n)*2654435761U; 
}


inline std::size_t hash(Int16 n)
{
	return static_cast<std::size_t>(n)*2654435761U; 
}


inline std::size_t hash(UInt16 n)
{
	return static_cast<std::size_t>(n)*2654435761U; 
}


inline std::size_t hash(Int32 n)
{
	return static_cast<std::size_t>(n)*2654435761U; 
}


inline std::size_t hash(UInt32 n)
{
	return static_cast<std::size_t>(n)*2654435761U; 
}


inline std::size_t hash(Int64 n)
{
	return static_cast<std::size_t>(n)*2654435761U; 
}


inline std::size_t hash(UInt64 n)
{
	return static_cast<std::size_t>(n)*2654435761U; 
}


} // namespace Poco


#endif // Foundation_Hash_INCLUDED
