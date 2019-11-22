#pragma once

#include <Common/HashTable/StringHashMap.h>
#include <Common/HashTable/TwoLevelStringHashTable.h>

template <typename TMapped, typename Allocator = HashTableAllocator, template <typename...> typename ImplTable = StringHashMap>
class TwoLevelStringHashMap : public TwoLevelStringHashTable<StringHashMapSubMaps<TMapped, Allocator>, ImplTable<TMapped, Allocator>>
{
public:
    using Key = StringRef;
    using key_type = Key;
    using Self = TwoLevelStringHashMap;
    using Base = TwoLevelStringHashTable<StringHashMapSubMaps<TMapped, Allocator>, StringHashMap<TMapped, Allocator>>;
    using Base::Base;
    using typename Base::Impl;
    using mapped_type = TMapped;
    using value_type = typename Base::value_type;

    using LookupResult = typename Base::LookupResult;

    template <typename Func>
    void ALWAYS_INLINE forEachMapped(Func && func)
    {
        for (auto i = 0u; i < this->NUM_BUCKETS; ++i)
            return this->impls[i].forEachMapped(func);
    }

    mapped_type & ALWAYS_INLINE operator[](Key x)
    {
        bool inserted;
        LookupResult it;
        emplace(x, it, inserted);
        if (inserted)
            new (lookupResultGetMapped(it)) mapped_type();
        return *lookupResultGetMapped(it);
    }
};
