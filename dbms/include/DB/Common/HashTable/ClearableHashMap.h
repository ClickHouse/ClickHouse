#pragma once

#include <DB/Common/HashTable/HashMap.h>
#include <DB/Common/HashTable/ClearableHashSet.h>


template <typename Key, typename Mapped, typename Hash>
using ClearableHashMapCell = ClearableHashTableCell<Key, HashMapCell<Key, Mapped, Hash, ClearableHashSetState>>;


template
<
	typename Key,
	typename Mapped,
	typename Hash = DefaultHash<Key>,
	typename Grower = HashTableGrower<>,
	typename Allocator = HashTableAllocator
>
class ClearableHashMap : public HashTable<Key, ClearableHashMapCell<Key, Mapped, Hash>, Hash, Grower, Allocator>
{
public:
	typedef Key key_type;
	typedef Mapped mapped_type;
	typedef typename ClearableHashMap::cell_type::value_type value_type;

	mapped_type & operator[](Key x)
	{
		typename ClearableHashMap::iterator it;
		bool inserted;
		this->emplace(x, it, inserted);

		if (inserted)
			new(&it->second) mapped_type();

		return it->second;
	}

	void clear()
	{
		++this->version;
		this->m_size = 0;
	}
};
