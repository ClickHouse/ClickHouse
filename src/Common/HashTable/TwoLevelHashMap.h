#pragma once

#include <Common/HashTable/TwoLevelHashTable.h>
#include <Common/HashTable/HashMap.h>


template <
    typename Key,
    typename Cell,
    typename Hash = DefaultHash<Key>,
    typename Grower = TwoLevelHashTableGrower<>,
    typename Allocator = HashTableAllocator,
    template <typename...> typename ImplTable = HashMapTable,
    size_t BITS_FOR_BUCKET = DEFAULT_BITS_FOR_BUCKET>
class TwoLevelHashMapTable
    : public TwoLevelHashTable<Key, Cell, Hash, Grower, Allocator, ImplTable<Key, Cell, Hash, Grower, Allocator>, BITS_FOR_BUCKET>
{
public:
    using Impl = ImplTable<Key, Cell, Hash, Grower, Allocator>;
    using Base = TwoLevelHashTable<Key, Cell, Hash, Grower, Allocator, ImplTable<Key, Cell, Hash, Grower, Allocator>, BITS_FOR_BUCKET>;
    using LookupResult = typename Impl::LookupResult;

    using Base::Base;
    using Base::prefetch;

    typename Cell::Mapped & ALWAYS_INLINE operator[](const Key & x)
    {
        LookupResult it;
        bool inserted = false;
        this->emplace(x, it, inserted);

        if (inserted)
            new (&it->getMapped()) typename Cell::Mapped();

        return it->getMapped();
    }
};


template <
    typename Key,
    typename Mapped,
    typename Hash = DefaultHash<Key>,
    typename Grower = TwoLevelHashTableGrower<>,
    typename Allocator = HashTableAllocator,
    template <typename...> typename ImplTable = HashMapTable,
    size_t BITS_FOR_BUCKET = DEFAULT_BITS_FOR_BUCKET>
using TwoLevelHashMap = TwoLevelHashMapTable<Key, HashMapCell<Key, Mapped, Hash>, Hash, Grower, Allocator, ImplTable, BITS_FOR_BUCKET>;


template <
    typename Key,
    typename Mapped,
    typename Hash = DefaultHash<Key>,
    typename Grower = TwoLevelHashTableGrower<>,
    typename Allocator = HashTableAllocator,
    template <typename...> typename ImplTable = HashMapTable,
    size_t BITS_FOR_BUCKET = DEFAULT_BITS_FOR_BUCKET>
using TwoLevelHashMapWithSavedHash
    = TwoLevelHashMapTable<Key, HashMapCellWithSavedHash<Key, Mapped, Hash>, Hash, Grower, Allocator, ImplTable, BITS_FOR_BUCKET>;
