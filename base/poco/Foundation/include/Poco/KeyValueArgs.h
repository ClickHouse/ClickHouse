//
// KeyValueArgs.h
//
// Library: Foundation
// Package: Cache
// Module:  KeyValueArgs
//
// Definition of the KeyValueArgs class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_KeyValueArgs_INCLUDED
#define Foundation_KeyValueArgs_INCLUDED


#include "Poco/Foundation.h"


namespace Poco {


template <class TKey, class TValue> 
class KeyValueArgs
	/// Simply event arguments class to transfer a key and a value via an event call.
	/// Note that key and value are *NOT* copied, only references to them are stored.
{
public:
	KeyValueArgs(const TKey& aKey, const TValue& aVal): 
		_key(aKey), 
		_value(aVal)
	{
	}

	KeyValueArgs(const KeyValueArgs& args):
		_key(args._key), 
		_value(args._value)
	{
	}

	~KeyValueArgs()
	{
	}

	const TKey& key() const
		/// Returns a reference to the key,
	{
		return _key;
	}

	const TValue& value() const
		/// Returns a Reference to the value.
	{
		return _value;
	}

protected:
	const TKey&   _key;
	const TValue& _value;

private:
	KeyValueArgs& operator = (const KeyValueArgs& args);
};


} // namespace Poco


#endif // Foundation_KeyValueArgs_INCLUDED
