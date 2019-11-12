#pragma once

#include <Common/HashTable/HashSet.h>
#include <Common/HashTable/HashTableAllocator.h>
#include <Common/HashTable/StringHashTable.h>

template <typename Key>
struct StringHashSetCell : public HashTableCell<Key, StringHashTableHash, HashTableNoState>
{
    using Base = HashTableCell<Key, StringHashTableHash, HashTableNoState>;
    using value_type = typename Base::value_type;
    using Base::Base;
    static constexpr bool need_zero_value_storage = false;
    // external
    const StringRef getKey() const { return toStringRef(this->key); }
    // internal
    static const Key & getKey(const value_type & value_) { return value_; }
};

template <>
struct StringHashSetCell<StringKey16> : public HashTableCell<StringKey16, StringHashTableHash, HashTableNoState>
{
    using Base = HashTableCell<StringKey16, StringHashTableHash, HashTableNoState>;
    using value_type = typename Base::value_type;
    using Base::Base;
    static constexpr bool need_zero_value_storage = false;
    bool isZero(const HashTableNoState & state) const { return isZero(this->key, state); }
    // Assuming String does not contain zero bytes. NOTE: Cannot be used in serialized method
    static bool isZero(const StringKey16 & key_, const HashTableNoState & /*state*/) { return key_.low == 0; }
    void setZero() { this->key.low = 0; }
    // external
    const StringRef getKey() const { return toStringRef(this->key); }
    // internal
    static const StringKey16 & getKey(const value_type & value_) { return value_; }
};

template <>
struct StringHashSetCell<StringKey24> : public HashTableCell<StringKey24, StringHashTableHash, HashTableNoState>
{
    using Base = HashTableCell<StringKey24, StringHashTableHash, HashTableNoState>;
    using value_type = typename Base::value_type;
    using Base::Base;
    static constexpr bool need_zero_value_storage = false;
    bool isZero(const HashTableNoState & state) const { return isZero(this->key, state); }
    // Assuming String does not contain zero bytes. NOTE: Cannot be used in serialized method
    static bool isZero(const StringKey24 & key_, const HashTableNoState & /*state*/) { return key_.a == 0; }
    void setZero() { this->key.a = 0; }
    // external
    const StringRef getKey() const { return toStringRef(this->key); }
    // internal
    static const StringKey24 & getKey(const value_type & value_) { return value_; }
};

template <>
struct StringHashSetCell<StringRef> : public HashSetCellWithSavedHash<StringRef, StringHashTableHash, HashTableNoState>
{
    using Base = HashSetCellWithSavedHash<StringRef, StringHashTableHash, HashTableNoState>;
    using value_type = typename Base::value_type;
    using Base::Base;
    static constexpr bool need_zero_value_storage = false;
    // external
    using Base::getKey;
    // internal
    static const StringRef & getKey(const value_type & value_) { return value_; }
};

template <typename Allocator>
struct StringHashSetSubSets
{
    using T0 = StringHashTableEmpty<StringHashSetCell<StringRef>>;
    using T1 = HashTable<StringKey8, StringHashSetCell<StringKey8>, StringHashTableHash, StringHashTableGrower<>, Allocator>;
    using T2 = HashTable<StringKey16, StringHashSetCell<StringKey16>, StringHashTableHash, StringHashTableGrower<>, Allocator>;
    using T3 = HashTable<StringKey24, StringHashSetCell<StringKey24>, StringHashTableHash, StringHashTableGrower<>, Allocator>;
    using Ts = HashTable<StringRef, StringHashSetCell<StringRef>, StringHashTableHash, StringHashTableGrower<>, Allocator>;
};

template <typename Allocator = HashTableAllocator>
class StringHashSet : public StringHashTable<StringHashSetSubSets<Allocator>>
{
public:
    using Key = StringRef;
    using Base = StringHashTable<StringHashSetSubSets<Allocator>>;
    using Self = StringHashSet;
    using LookupResult = typename Base::LookupResult;

    using Base::Base;
};
