#pragma once

#include <Common/HashTable/HashSet.h>
#include <Common/HashTable/StringHashTable.h>

template <typename Key>
struct StringHashSetCell : public HashTableCell<Key, StringHashTableHash, HashTableNoState>
{
    using Base = HashTableCell<Key, StringHashTableHash, HashTableNoState>;
    using Base::Base;
    using Base::getKey;
    static constexpr bool need_zero_value_storage = false;
    // external
    const StringRef getKey() const { return toStringRef(this->key); }
    // internal
    static const Key & getKey(const Key & value) { return value; }
};

template <>
struct StringHashSetCell<StringKey16> : public HashTableCell<StringKey16, StringHashTableHash, HashTableNoState>
{
    using Base = HashTableCell<StringKey16, StringHashTableHash, HashTableNoState>;
    using Base::Base;
    static constexpr bool need_zero_value_storage = false;
    bool isZero(const HashTableNoState & state) const { return isZero(this->key, state); }
    // Assuming String does not contain zero bytes. NOTE: Cannot be used in serialized method
    static bool isZero(const StringKey16 & key, const HashTableNoState & /*state*/) { return key.low == 0; }
    void setZero() { this->key.low = 0; }
    // external
    const StringRef getKey() const { return toStringRef(this->key); }
    // internal
    static const StringKey16 & getKey(const value_type & value) { return value; }
};

template <>
struct StringHashSetCell<StringKey24> : public HashTableCell<StringKey24, StringHashTableHash, HashTableNoState>
{
    using Base = HashTableCell<StringKey24, StringHashTableHash, HashTableNoState>;
    using Base::Base;
    static constexpr bool need_zero_value_storage = false;
    bool isZero(const HashTableNoState & state) const { return isZero(this->key, state); }
    // Assuming String does not contain zero bytes. NOTE: Cannot be used in serialized method
    static bool isZero(const StringKey24 & key, const HashTableNoState & /*state*/) { return key.a == 0; }
    void setZero() { this->key.a = 0; }
    // external
    const StringRef getKey() const { return toStringRef(this->key); }
    // internal
    static const StringKey24 & getKey(const value_type & value) { return value; }
};

template <>
struct StringHashSetCell<StringRef> : public HashSetCellWithSavedHash<StringRef, StringHashTableHash, HashTableNoState>
{
    using Base = HashSetCellWithSavedHash<StringRef, StringHashTableHash, HashTableNoState>;
    using Base::Base;
    static constexpr bool need_zero_value_storage = false;
    // internal
    static const StringRef & getKey(const value_type & value) { return value; }
};

template <typename Allocator>
struct StringHashSetSubMaps
{
    using T0 = StringHashTableEmpty<StringHashSetCell<StringRef>>;
    using T1 = HashTable<StringKey8, StringHashSetCell<StringKey8>, StringHashTableHash, StringHashTableGrower<>, Allocator>;
    using T2 = HashTable<StringKey16, StringHashSetCell<StringKey16>, StringHashTableHash, StringHashTableGrower<>, Allocator>;
    using T3 = HashTable<StringKey24, StringHashSetCell<StringKey24>, StringHashTableHash, HashTableGrower<>, Allocator>;
    using Ts = HashTable<StringRef, StringHashSetCell<StringRef>, StringHashTableHash, HashTableGrower<>, Allocator>;
};

template <typename Allocator = HashTableAllocator>
class StringHashSet : public StringHashTable<StringHashSetSubMaps<Allocator>>
{
public:
    using Self = StringHashSet;
    using Base = StringHashTable<StringHashSetSubMaps<Allocator>>;

    void merge(const Self & rhs)
    {
        if (rhs.m0.size())
            this->m0.insert(rhs.m0.value);
        this->m1.merge(rhs.m1);
        this->m2.merge(rhs.m2);
        this->m3.merge(rhs.m3);
        this->ms.merge(rhs.ms);
    }
};
