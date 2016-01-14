//
// StrategyCollection.h
//
// $Id: //poco/1.4/Foundation/include/Poco/StrategyCollection.h#1 $
//
// Library: Foundation
// Package: Cache
// Module:  StrategyCollection
//
// Definition of the StrategyCollection class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_StrategyCollection_INCLUDED
#define Foundation_StrategyCollection_INCLUDED


#include "Poco/KeyValueArgs.h"
#include "Poco/ValidArgs.h"
#include "Poco/AbstractStrategy.h"
#include "Poco/SharedPtr.h"
#include <vector>


namespace Poco {


template <class TKey, class TValue> 
class StrategyCollection: public AbstractStrategy<TKey, TValue>
	/// An StrategyCollection is a decorator masking n collections as a single one.
{
public:
	typedef std::vector<SharedPtr<AbstractStrategy<TKey, TValue> > > Strategies;
	typedef typename Strategies::iterator       Iterator;
	typedef typename Strategies::const_iterator ConstIterator;

public:
	StrategyCollection()
	{
	}

	~StrategyCollection()
	{
	}

	void pushBack(AbstractStrategy<TKey, TValue>* pStrat)
		/// Adds an AbstractStrategy to the collection. Class takes ownership of pointer
	{
		_strategies.push_back(SharedPtr<AbstractStrategy<TKey, TValue> >(pStrat));
	}

	void popBack()
		/// Removes the last added AbstractStrategy from the collection.
	{
		_strategies.pop_back();
	}

	void onAdd(const void* pSender, const KeyValueArgs <TKey, TValue>& key)
		/// Adds the key to the strategy.
		/// If for the key already an entry exists, it will be overwritten.
	{
		Iterator it = _strategies.begin();
		Iterator endIt = _strategies.end();
		for (; it != endIt; ++it)
		{
			(*it)->onAdd(pSender, key);
		}
	}

	void onRemove(const void* pSender, const TKey& key)
		/// Removes an entry from the strategy. If the entry is not found
		/// the remove is ignored.
	{
		Iterator it = _strategies.begin();
		Iterator endIt = _strategies.end();
		for (; it != endIt; ++it)
		{
			(*it)->onRemove(pSender, key);
		}
	}

	void onGet(const void* pSender, const TKey& key)
	{
		Iterator it = _strategies.begin();
		Iterator endIt = _strategies.end();
		for (; it != endIt; ++it)
		{
			(*it)->onGet(pSender, key);
		}
	}

	void onClear(const void* pSender, const EventArgs& args)
	{
		Iterator it = _strategies.begin();
		Iterator endIt = _strategies.end();
		for (; it != endIt; ++it)
		{
			(*it)->onClear(pSender, args);
		}
	}

	void onIsValid(const void* pSender, ValidArgs<TKey>& key)
	{
		Iterator it = _strategies.begin();
		Iterator endIt = _strategies.end();
		for (; it != endIt && key.isValid(); ++it)
		{
			(*it)->onIsValid(pSender, key);
		}
	}

	void onReplace(const void* pSender, std::set<TKey>& elemsToRemove)
	{
		Iterator it = _strategies.begin();
		Iterator endIt = _strategies.end();
		for (; it != endIt; ++it)
		{
			(*it)->onReplace(pSender, elemsToRemove);
		}
	}

protected:
	Strategies _strategies;
};


} // namespace Poco


#endif // Foundation_StrategyCollection_INCLUDED
