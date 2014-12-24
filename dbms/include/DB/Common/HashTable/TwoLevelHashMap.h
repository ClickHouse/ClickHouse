#pragma once

#include <DB/Common/HashTable/TwoLevelHashTable.h>
#include <DB/Common/HashTable/HashMap.h>


template
<
	typename Key,
	typename Cell,
	typename Hash = DefaultHash<Key>,
	typename Grower = HashTableGrower<8>,
	typename Allocator = HashTableAllocator
>
class TwoLevelHashMapTable : public TwoLevelHashTable<Key, Cell, Hash, Grower, Allocator, HashMapTable<Key, Cell, Hash, Grower, Allocator> >
{
public:
	typedef Key key_type;
	typedef typename Cell::Mapped mapped_type;
	typedef typename Cell::value_type value_type;

	mapped_type & operator[](Key x)
	{
		typename TwoLevelHashMapTable::iterator it;
		bool inserted;
		this->emplace(x, it, inserted);

		if (!__has_trivial_constructor(mapped_type) && inserted)
			new(&it->second) mapped_type();

		return it->second;
	}
};


template
<
	typename Key,
	typename Mapped,
	typename Hash = DefaultHash<Key>,
	typename Grower = HashTableGrower<8>,
	typename Allocator = HashTableAllocator
>
using TwoLevelHashMap = TwoLevelHashMapTable<Key, HashMapCell<Key, Mapped, Hash>, Hash, Grower, Allocator>;


template
<
	typename Key,
	typename Mapped,
	typename Hash = DefaultHash<Key>,
	typename Grower = HashTableGrower<8>,
	typename Allocator = HashTableAllocator
>
using TwoLevelHashMapWithSavedHash =  TwoLevelHashMapTable<Key, HashMapCellWithSavedHash<Key, Mapped, Hash>, Hash, Grower, Allocator>;
