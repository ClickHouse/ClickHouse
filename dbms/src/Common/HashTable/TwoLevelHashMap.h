#pragma once

#include <Common/HashTable/TwoLevelHashTable.h>
#include <Common/HashTable/HashMap.h>


template
<
    typename Key,
    typename Cell,
    typename Hash = DefaultHash<Key>,
    typename Grower = TwoLevelHashTableGrower<>,
    typename Allocator = HashTableAllocator,
    template <typename ...> typename ImplTable = HashMapTable
>
class TwoLevelHashMapTable : public TwoLevelHashTable<Key, Cell, Hash, Grower, Allocator, ImplTable<Key, Cell, Hash, Grower, Allocator>>
{
public:
    using key_type = Key;
    using mapped_type = typename Cell::Mapped;
    using value_type = typename Cell::value_type;

    using Impl = ImplTable<Key, Cell, Hash, Grower, Allocator>;
    using LookupResult = typename Impl::LookupResult;

    using TwoLevelHashTable<Key, Cell, Hash, Grower, Allocator, ImplTable<Key, Cell, Hash, Grower, Allocator>>::TwoLevelHashTable;

    template <typename Func>
    void ALWAYS_INLINE forEachMapped(Func && func)
    {
        for (auto i = 0u; i < this->NUM_BUCKETS; ++i)
            this->impls[i].forEachMapped(func);
    }

    mapped_type & ALWAYS_INLINE operator[](Key x)
    {
        typename TwoLevelHashMapTable::LookupResult it;
        bool inserted;
        this->emplace(x, it, inserted);

        if (inserted)
            new(lookupResultGetMapped(it)) mapped_type();

        return *lookupResultGetMapped(it);
    }
};


template
<
    typename Key,
    typename Mapped,
    typename Hash = DefaultHash<Key>,
    typename Grower = TwoLevelHashTableGrower<>,
    typename Allocator = HashTableAllocator,
    template <typename ...> typename ImplTable = HashMapTable
>
using TwoLevelHashMap = TwoLevelHashMapTable<Key, HashMapCell<Key, Mapped, Hash>, Hash, Grower, Allocator, ImplTable>;


template
<
    typename Key,
    typename Mapped,
    typename Hash = DefaultHash<Key>,
    typename Grower = TwoLevelHashTableGrower<>,
    typename Allocator = HashTableAllocator,
    template <typename ...> typename ImplTable = HashMapTable
>
using TwoLevelHashMapWithSavedHash = TwoLevelHashMapTable<Key, HashMapCellWithSavedHash<Key, Mapped, Hash>, Hash, Grower, Allocator, ImplTable>;
