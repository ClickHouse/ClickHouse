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
    using mapped_type = TMapped;
    using value_type = typename Base::value_type;

    mapped_type & ALWAYS_INLINE operator[](Key x)
    {
        typename Base::LookupResult it;
        bool inserted;
        this->emplace(x, it, inserted);

        if (inserted)
            new (it->getSecond()) mapped_type();

        return *it->getSecond();
    }
};
