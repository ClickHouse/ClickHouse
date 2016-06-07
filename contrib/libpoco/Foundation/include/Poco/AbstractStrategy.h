//
// AbstractStrategy.h
//
// $Id: //poco/1.4/Foundation/include/Poco/AbstractStrategy.h#1 $
//
// Library: Foundation
// Package: Cache
// Module:  AbstractCache
//
// Definition of the AbstractStrategy class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_AbstractStrategy_INCLUDED
#define Foundation_AbstractStrategy_INCLUDED


#include "Poco/KeyValueArgs.h"
#include "Poco/ValidArgs.h"
#include "Poco/EventArgs.h"
#include <set>


namespace Poco {


template <class TKey, class TValue> 
class AbstractStrategy
	/// An AbstractStrategy is the interface for all strategies. 
{
public:
	AbstractStrategy()
	{
	}

	virtual ~AbstractStrategy()
	{
	}

	virtual void onUpdate(const void* pSender, const KeyValueArgs <TKey, TValue>& args)
		/// Updates an existing entry.
	{
		onRemove(pSender,args.key());
		onAdd(pSender, args);
	}
	
	virtual void onAdd(const void* pSender, const KeyValueArgs <TKey, TValue>& key) = 0;
		/// Adds the key to the strategy.
		/// If for the key already an entry exists, an exception will be thrown.

	virtual void onRemove(const void* pSender, const TKey& key) = 0;
		/// Removes an entry from the strategy. If the entry is not found
		/// the remove is ignored.

	virtual void onGet(const void* pSender, const TKey& key) = 0;
		/// Informs the strategy that a read-access happens to an element.

	virtual void onClear(const void* pSender, const EventArgs& args) = 0;
		/// Removes all elements from the cache.

	virtual void onIsValid(const void* pSender, ValidArgs<TKey>& key) = 0;
		/// Used to query if a key is still valid (i.e. cached).

	virtual void onReplace(const void* pSender, std::set<TKey>& elemsToRemove) = 0;
		/// Used by the Strategy to indicate which elements should be removed from
		/// the cache. Note that onReplace does not change the current list of keys.
		/// The cache object is reponsible to remove the elements.
};


} // namespace Poco


#endif // Foundation_AbstractStrategy_INCLUDED
