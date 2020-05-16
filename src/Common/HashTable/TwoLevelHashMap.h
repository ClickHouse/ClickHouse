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
    template <typename ...> typename ImplTable = HashMapTable,
    size_t BITS_FOR_BUCKET = 8
>
class TwoLevelHashMapTable : public TwoLevelHashTable<Key, Cell, Hash, Grower, Allocator, ImplTable<Key, Cell, Hash, Grower, Allocator>, BITS_FOR_BUCKET>
{
public:
    using Impl = ImplTable<Key, Cell, Hash, Grower, Allocator>;
    using LookupResult = typename Impl::LookupResult;

    using TwoLevelHashTable<Key, Cell, Hash, Grower, Allocator, ImplTable<Key, Cell, Hash, Grower, Allocator>, BITS_FOR_BUCKET>::TwoLevelHashTable;

    template <typename Func>
    void ALWAYS_INLINE forEachMapped(Func && func)
    {
        for (auto i = 0u; i < this->NUM_BUCKETS; ++i)
            this->impls[i].forEachMapped(func);
    }

    typename Cell::Mapped & ALWAYS_INLINE operator[](const Key & x)
    {
        LookupResult it;
        bool inserted;
        this->emplace(x, it, inserted);

        if (inserted)
            new (&it->getMapped()) typename Cell::Mapped();

        return it->getMapped();
    }
};


template
<
    typename Key,
    typename Mapped,
    typename Hash = DefaultHash<Key>,
    typename Grower = TwoLevelHashTableGrower<>,
    typename Allocator = HashTableAllocator,
    template <typename ...> typename ImplTable = HashMapTable,
    size_t BITS_FOR_BUCKET = 8
>
using TwoLevelHashMap = TwoLevelHashMapTable<Key, HashMapCell<Key, Mapped, Hash>, Hash, Grower, Allocator, ImplTable, BITS_FOR_BUCKET>;

template
<
    typename Key,
    typename Mapped,
    typename Hash = DefaultHash<Key>,
    typename Grower = TwoLevelHashTableGrower<>,
    typename Allocator = HashTableAllocator,
    template <typename ...> typename ImplTable = HashMapTable,
    size_t BITS_FOR_BUCKET = 11
>
using TwoLevelSharedHashMap = TwoLevelHashMap<Key, Mapped, Hash, Grower, Allocator, ImplTable, BITS_FOR_BUCKET>;


template
<
    typename Key,
    typename Mapped,
    typename Hash = DefaultHash<Key>,
    typename Grower = TwoLevelHashTableGrower<>,
    typename Allocator = HashTableAllocator,
    template <typename ...> typename ImplTable = HashMapTable,
    size_t BITS_FOR_BUCKET = 8
>
using TwoLevelHashMapWithSavedHash = TwoLevelHashMapTable<Key, HashMapCellWithSavedHash<Key, Mapped, Hash>, Hash, Grower, Allocator, ImplTable, BITS_FOR_BUCKET>;

template
<
    typename Key,
    typename Mapped,
    typename Hash = DefaultHash<Key>,
    typename Grower = TwoLevelHashTableGrower<>,
    typename Allocator = HashTableAllocator,
    template <typename ...> typename ImplTable = HashMapTable,
    size_t BITS_FOR_BUCKET = 11
>
using TwoLevelSharedHashMapWithSavedHash = TwoLevelHashMapWithSavedHash<Key, Mapped, Hash, Grower, Allocator, ImplTable, BITS_FOR_BUCKET>;
