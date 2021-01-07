#pragma once

#include <Common/HashTable/HashSet.h>
#include <Common/HashTable/HashTableAllocator.h>
#include <Common/HashTable/StringHashTable.h>

template <typename Key>
struct StringHashSetCell : public HashTableCell<Key, StringHashTableHash, HashTableNoState>
{
    using Base = HashTableCell<Key, StringHashTableHash, HashTableNoState>;
    using Base::Base;

    VoidMapped void_map;
    VoidMapped & getMapped() { return void_map; }
    const VoidMapped & getMapped() const { return void_map; }

    static constexpr bool need_zero_value_storage = false;
};

template <>
struct StringHashSetCell<StringKey16> : public HashTableCell<StringKey16, StringHashTableHash, HashTableNoState>
{
    using Base = HashTableCell<StringKey16, StringHashTableHash, HashTableNoState>;
    using Base::Base;

    VoidMapped void_map;
    VoidMapped & getMapped() { return void_map; }
    const VoidMapped & getMapped() const { return void_map; }

    static constexpr bool need_zero_value_storage = false;

    bool isZero(const HashTableNoState & state) const { return isZero(this->key, state); }
    // Zero means unoccupied cells in hash table. Use key with last word = 0 as
    // zero keys, because such keys are unrepresentable (no way to encode length).
    static bool isZero(const StringKey16 & key_, const HashTableNoState &)
    { return key_.high == 0; }
    void setZero() { this->key.high = 0; }
};

template <>
struct StringHashSetCell<StringKey24> : public HashTableCell<StringKey24, StringHashTableHash, HashTableNoState>
{
    using Base = HashTableCell<StringKey24, StringHashTableHash, HashTableNoState>;
    using Base::Base;

    VoidMapped void_map;
    VoidMapped & getMapped() { return void_map; }
    const VoidMapped & getMapped() const { return void_map; }

    static constexpr bool need_zero_value_storage = false;

    bool isZero(const HashTableNoState & state) const { return isZero(this->key, state); }
    // Zero means unoccupied cells in hash table. Use key with last word = 0 as
    // zero keys, because such keys are unrepresentable (no way to encode length).
    static bool isZero(const StringKey24 & key_, const HashTableNoState &)
    { return key_.c == 0; }
    void setZero() { this->key.c = 0; }
};

template <>
struct StringHashSetCell<StringRef> : public HashSetCellWithSavedHash<StringRef, StringHashTableHash, HashTableNoState>
{
    using Base = HashSetCellWithSavedHash<StringRef, StringHashTableHash, HashTableNoState>;
    using Base::Base;

    VoidMapped void_map;
    VoidMapped & getMapped() { return void_map; }
    const VoidMapped & getMapped() const { return void_map; }

    static constexpr bool need_zero_value_storage = false;
};

template <typename Allocator>
struct StringHashSetSubMaps
{
    using T0 = StringHashTableEmpty<StringHashSetCell<StringRef>>;
    using T1 = HashSetTable<StringKey8, StringHashSetCell<StringKey8>, StringHashTableHash, StringHashTableGrower<>, Allocator>;
    using T2 = HashSetTable<StringKey16, StringHashSetCell<StringKey16>, StringHashTableHash, StringHashTableGrower<>, Allocator>;
    using T3 = HashSetTable<StringKey24, StringHashSetCell<StringKey24>, StringHashTableHash, StringHashTableGrower<>, Allocator>;
    using Ts = HashSetTable<StringRef, StringHashSetCell<StringRef>, StringHashTableHash, StringHashTableGrower<>, Allocator>;
};

template <typename Allocator = HashTableAllocator>
class StringHashSet : public StringHashTable<StringHashSetSubMaps<Allocator>>
{
public:
    using Key = StringRef;
    using Base = StringHashTable<StringHashSetSubMaps<Allocator>>;
    using Self = StringHashSet;
    using LookupResult = typename Base::LookupResult;

    using Base::Base;

    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder && key_holder, bool & inserted)
    {
        LookupResult it;
        Base::emplace(key_holder, it, inserted);
    }

};
