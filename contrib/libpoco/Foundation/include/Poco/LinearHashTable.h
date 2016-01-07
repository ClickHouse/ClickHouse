//
// LinearHashTable.h
//
// $Id: //poco/1.4/Foundation/include/Poco/LinearHashTable.h#1 $
//
// Library: Foundation
// Package: Hashing
// Module:  LinearHashTable
//
// Definition of the LinearHashTable class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_LinearHashTable_INCLUDED
#define Foundation_LinearHashTable_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Hash.h"
#include <functional>
#include <algorithm>
#include <vector>
#include <utility>
#include <cstddef>


namespace Poco {


template <class Value, class HashFunc = Hash<Value> >
class LinearHashTable
	/// This class implements a linear hash table.
	///
	/// In a linear hash table, the available address space
	/// grows or shrinks dynamically. A linar hash table thus
	/// supports any number of insertions or deletions without
	/// lookup or insertion performance deterioration.
	///
	/// Linear hashing was discovered by Witold Litwin in 1980
	/// and described in the paper LINEAR HASHING: A NEW TOOL FOR FILE AND TABLE ADDRESSING.
	///
	/// For more information on linear hashing, see <http://en.wikipedia.org/wiki/Linear_hash>.
	///
	/// The LinearHashTable is not thread safe.
	///
	/// Value must support comparison for equality.
	///
	/// Find, insert and delete operations are basically O(1) with regard
	/// to the total number of elements in the table, and O(N) with regard
	/// to the number of elements in the bucket where the element is stored.
	/// On average, every bucket stores one element; the exact number depends
	/// on the quality of the hash function. In most cases, the maximum number of
	/// elements in a bucket should not exceed 3.
{
public:
	typedef Value               ValueType;
	typedef Value&              Reference;
	typedef const Value&        ConstReference;
	typedef Value*              Pointer;
	typedef const Value*        ConstPointer;
	typedef HashFunc            Hash;
	typedef std::vector<Value>  Bucket;
	typedef std::vector<Bucket> BucketVec;
	typedef typename Bucket::iterator    BucketIterator;
	typedef typename BucketVec::iterator BucketVecIterator;

	class ConstIterator: public std::iterator<std::forward_iterator_tag, Value>
	{
	public:
		ConstIterator(): _initialized(false)
		{
		}
		
		ConstIterator(const BucketVecIterator& vecIt, const BucketVecIterator& endIt, const BucketIterator& buckIt):
			_vecIt(vecIt),
			_endIt(endIt),
			_buckIt(buckIt),
			_initialized(true)
		{
		}

		ConstIterator(const ConstIterator& it):
			_vecIt(it._vecIt),
			_endIt(it._endIt),
			_buckIt(it._buckIt),
			_initialized(it._initialized)

		{
		}
		
		ConstIterator& operator = (const ConstIterator& it)
		{
			ConstIterator tmp(it);
			swap(tmp);
			return *this;
		}
		
		void swap(ConstIterator& it)
		{
			using std::swap;
			// uninitialized iterators crash when swapped
			if (_initialized)
			{
				swap(_vecIt, it._vecIt);
				swap(_endIt, it._endIt);
				swap(_buckIt, it._buckIt);
				swap(_initialized, it._initialized);
			}
			else 
			{
				_vecIt = it._vecIt;
				_endIt = it._endIt;
				_buckIt = it._buckIt;
				_initialized = it._initialized;
			}
		}
		
		bool operator == (const ConstIterator& it) const
		{
			return _vecIt == it._vecIt && (_vecIt == _endIt || _buckIt == it._buckIt);
		}

		bool operator != (const ConstIterator& it) const
		{
			return _vecIt != it._vecIt || (_vecIt != _endIt && _buckIt != it._buckIt);
		}
		
		const typename Bucket::value_type& operator * () const
		{
			return *_buckIt;
		}

		const typename Bucket::value_type* operator -> () const
		{
			return &*_buckIt;
		}
		
		ConstIterator& operator ++ () // prefix
		{
			if (_vecIt != _endIt)
			{
				++_buckIt;
				while (_vecIt != _endIt && _buckIt == _vecIt->end())
				{
					++_vecIt;
					if (_vecIt != _endIt) _buckIt = _vecIt->begin();
				}
			}
			return *this;
		}
		
		ConstIterator operator ++ (int) // postfix
		{
			ConstIterator tmp(*this);
			++*this;
			return tmp;
		}
		
	protected:
		BucketVecIterator _vecIt;
		BucketVecIterator _endIt;
		BucketIterator    _buckIt;
		bool              _initialized;
		
		friend class LinearHashTable;
	};
	
	class Iterator: public ConstIterator
	{
	public:
		Iterator()
		{
		}
		
		Iterator(const BucketVecIterator& vecIt, const BucketVecIterator& endIt, const BucketIterator& buckIt):
			ConstIterator(vecIt, endIt, buckIt)
		{
		}

		Iterator(const Iterator& it):
			ConstIterator(it)
		{
		}
		
		Iterator& operator = (const Iterator& it)
		{
			Iterator tmp(it);
			ConstIterator::swap(tmp);
			return *this;
		}
		
		void swap(Iterator& it)
		{
			ConstIterator::swap(it);
		}
				
		typename Bucket::value_type& operator * ()
		{
			return *this->_buckIt;
		}

		const typename Bucket::value_type& operator * () const
		{
			return *this->_buckIt;
		}

		typename Bucket::value_type* operator -> ()
		{
			return &*this->_buckIt;
		}

		const typename Bucket::value_type* operator -> () const
		{
			return &*this->_buckIt;
		}
		
		Iterator& operator ++ () // prefix
		{
			ConstIterator::operator ++ ();
			return *this;
		}
		
		Iterator operator ++ (int) // postfix
		{
			Iterator tmp(*this);
			++*this;
			return tmp;
		}

		friend class LinearHashTable;
	};
	
	LinearHashTable(std::size_t initialReserve = 64): 
		_split(0),
		_front(1),
		_size(0)
		/// Creates the LinearHashTable, using the given initialReserve.
	{
		_buckets.reserve(calcSize(initialReserve));
		_buckets.push_back(Bucket());
	}
	
	LinearHashTable(const LinearHashTable& table):
		_buckets(table._buckets),
		_split(table._split),
		_front(table._front),
		_size(table._size)
		/// Creates the LinearHashTable by copying another one.
	{
	}
	
	~LinearHashTable()
		/// Destroys the LinearHashTable.
	{
	}
	
	LinearHashTable& operator = (const LinearHashTable& table)
		/// Assigns another LinearHashTable.
	{
		LinearHashTable tmp(table);
		swap(tmp);
		return *this;
	}
	
	void swap(LinearHashTable& table)
		/// Swaps the LinearHashTable with another one.
	{
		using std::swap;
		swap(_buckets, table._buckets);
		swap(_split, table._split);
		swap(_front, table._front);
		swap(_size, table._size);
	}
	
	ConstIterator begin() const
		/// Returns an iterator pointing to the first entry, if one exists.
	{
		BucketVecIterator it(_buckets.begin());
		BucketVecIterator end(_buckets.end());
		while (it != end && it->empty())
		{
			++it;
		}
		if (it == end)
			return this->end();
		else
			return ConstIterator(it, end, it->begin());
	}
	
	ConstIterator end() const
		/// Returns an iterator pointing to the end of the table.
	{
		return ConstIterator(_buckets.end(), _buckets.end(), _buckets.front().end());
	}
	
	Iterator begin()
		/// Returns an iterator pointing to the first entry, if one exists.
	{
		BucketVecIterator it(_buckets.begin());
		BucketVecIterator end(_buckets.end());
		while (it != end && it->empty())
		{
			++it;
		}
		if (it == end)
			return this->end();
		else
			return Iterator(it, end, it->begin());
	}
	
	Iterator end()
		/// Returns an iterator pointing to the end of the table.
	{
		return Iterator(_buckets.end(), _buckets.end(), _buckets.front().end());
	}
		
	ConstIterator find(const Value& value) const
		/// Finds an entry in the table.
	{
		std::size_t addr = bucketAddress(value);
		BucketVecIterator it(_buckets.begin() + addr);
		BucketIterator buckIt(std::find(it->begin(), it->end(), value));
		if (buckIt != it->end())
			return ConstIterator(it, _buckets.end(), buckIt);
		else
			return end();
	}

	Iterator find(const Value& value)
		/// Finds an entry in the table.
	{
		std::size_t addr = bucketAddress(value);
		BucketVecIterator it(_buckets.begin() + addr);
		BucketIterator buckIt(std::find(it->begin(), it->end(), value));
		if (buckIt != it->end())
			return Iterator(it, _buckets.end(), buckIt);
		else
			return end();
	}
	
	std::size_t count(const Value& value) const
		/// Returns the number of elements with the given
		/// value, with is either 1 or 0.
	{
		return find(value) != end() ? 1 : 0;
	}
	
	std::pair<Iterator, bool> insert(const Value& value)
		/// Inserts an element into the table.
		///
		/// If the element already exists in the table,
		/// a pair(iterator, false) with iterator pointing to the 
		/// existing element is returned.
		/// Otherwise, the element is inserted an a 
		/// pair(iterator, true) with iterator
		/// pointing to the new element is returned.
	{
		std::size_t hash = _hash(value);
		std::size_t addr = bucketAddressForHash(hash);
		BucketVecIterator it(_buckets.begin() + addr);
		BucketIterator buckIt(std::find(it->begin(), it->end(), value));
		if (buckIt == it->end())
		{
			split();
			addr = bucketAddressForHash(hash);
			it = _buckets.begin() + addr;
			buckIt = it->insert(it->end(), value);
			++_size;
			return std::make_pair(Iterator(it, _buckets.end(), buckIt), true);
		}
		else
		{
			return std::make_pair(Iterator(it, _buckets.end(), buckIt), false);
		}
	}
	
	void erase(Iterator it)
		/// Erases the element pointed to by it.
	{
		if (it != end())
		{
			it._vecIt->erase(it._buckIt);
			--_size;
			merge();
		}
	}
	
	void erase(const Value& value)
		/// Erases the element with the given value, if it exists.
	{
		Iterator it = find(value);
		erase(it);
	}
	
	void clear()
		/// Erases all elements.
	{
		LinearHashTable empty;
		swap(empty);
	}
	
	std::size_t size() const
		/// Returns the number of elements in the table.
	{
		return _size;
	}
	
	bool empty() const
		/// Returns true iff the table is empty.
	{
		return _size == 0;
	}
	
	std::size_t buckets() const
		/// Returns the number of allocated buckets.
	{
		return _buckets.size();
	}
	
protected:
	std::size_t bucketAddress(const Value& value) const
	{
		std::size_t n = _hash(value);
		if (n % _front >= _split)
			return n % _front;
		else
			return n % (2*_front);
	}
	
	std::size_t bucketAddressForHash(std::size_t hash)
	{
		if (hash % _front >= _split)
			return hash % _front;
		else
			return hash % (2*_front);
	}
	
	void split()
	{
		if (_split == _front)
		{
			_split = 0;
			_front *= 2;
			_buckets.reserve(_front*2);
		}
		Bucket tmp;
		_buckets.push_back(tmp);
		_buckets[_split].swap(tmp);
		++_split;
		for (BucketIterator it = tmp.begin(); it != tmp.end(); ++it)
		{
			using std::swap;
			std::size_t addr = bucketAddress(*it);
			_buckets[addr].push_back(Value());
			swap(*it, _buckets[addr].back());
		}
	}
	
	void merge()
	{
		if (_split == 0)
		{
			_front /= 2;
			_split = _front;
		}
		--_split;
		Bucket tmp;
		tmp.swap(_buckets.back());
		_buckets.pop_back();
		for (BucketIterator it = tmp.begin(); it != tmp.end(); ++it)
		{
			using std::swap;
			std::size_t addr = bucketAddress(*it);
			_buckets[addr].push_back(Value());
			swap(*it, _buckets[addr].back());
		}
	}
	
	static std::size_t calcSize(std::size_t initialSize)
	{
		std::size_t size = 32;
		while (size < initialSize) size *= 2;
		return size;
	}
	
private:
	// Evil hack: _buckets must be mutable because both ConstIterator and Iterator hold 
	// ordinary iterator's (not const_iterator's).
	mutable BucketVec _buckets;
	std::size_t _split;
	std::size_t _front;
	std::size_t _size;
	HashFunc    _hash;
};


} // namespace Poco


#endif // Foundation_LinearHashTable_INCLUDED
