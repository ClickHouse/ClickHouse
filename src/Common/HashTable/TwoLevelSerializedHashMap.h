#pragma once

#include <Common/HashTable/SerializedHashMap.h>
#include <Common/HashTable/TwoLevelSerializedHashTable.h>

template <typename TMapped, typename Allocator = HashTableAllocator, template <typename...> typename ImplTable = SerializedHashMap>
class TwoLevelSerializedHashMap
    : public TwoLevelSerializedHashTable<SerializedHashMapSubMaps<TMapped, Allocator>, ImplTable<TMapped, Allocator>>
{
public:
    using Key = StringRef;
    using Self = TwoLevelSerializedHashMap;
    using Base = TwoLevelSerializedHashTable<SerializedHashMapSubMaps<TMapped, Allocator>, ImplTable<TMapped, Allocator>>;
    using LookupResult = typename Base::LookupResult;

    using Base::Base;

    template <typename Func>
    void ALWAYS_INLINE forEachMapped(Func && func)
    {
        for (auto i = 0u; i < this->NUM_BUCKETS; ++i)
            this->impls[i].forEachMapped(func);
    }

    TMapped & ALWAYS_INLINE operator[](const Key & x)
    {
        bool inserted;
        LookupResult it;
        this->emplace(x, it, inserted);
        if (inserted)
            new (&it->getMapped()) TMapped();
        return it->getMapped();
    }
};
