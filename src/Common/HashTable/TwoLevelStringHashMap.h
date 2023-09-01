#pragma once

#include <Common/HashTable/StringHashMap.h>
#include <Common/HashTable/TwoLevelStringHashTable.h>

template <typename TMapped, typename Allocator = HashTableAllocator, template <typename...> typename ImplTable = StringHashMap>
class TwoLevelStringHashMap : public TwoLevelStringHashTable<StringHashMapSubMaps<TMapped, Allocator>, ImplTable<TMapped, Allocator>>
{
public:
    using Key = StringRef;
    using Self = TwoLevelStringHashMap;
    using Base = TwoLevelStringHashTable<StringHashMapSubMaps<TMapped, Allocator>, StringHashMap<TMapped, Allocator>>;
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
