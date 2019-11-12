#pragma once

#include <Common/HashTable/ClearableHashSet.h>
#include <Common/HashTable/HashTableAllocator.h>
#include <Common/HashTable/StringHashTable.h>

template <typename Key>
struct ClearableStringHashSetCell : public ClearableHashTableCell<Key, HashTableCell<Key, StringHashTableHash, ClearableHashSetState>>
{
    using Base = ClearableHashTableCell<Key, HashTableCell<Key, StringHashTableHash, ClearableHashSetState>>;
    using value_type = typename Base::value_type;
    using Base::Base;
    // external
    const StringRef getKey() const { return toStringRef(this->key); }
    // internal
    static const Key & getKey(const value_type & value_) { return value_; }
};

template <>
struct ClearableStringHashSetCell<StringRef>
    : public ClearableHashTableCell<StringRef, HashSetCellWithSavedHash<StringRef, StringHashTableHash, ClearableHashSetState>>
{
    using Base = ClearableHashTableCell<StringRef, HashSetCellWithSavedHash<StringRef, StringHashTableHash, ClearableHashSetState>>;
    using value_type = typename Base::value_type;
    using Base::Base;
    // external
    using Base::getKey;
    // internal
    static const StringRef & getKey(const value_type & value_) { return value_; }
};

template <typename Allocator>
struct ClearableStringHashSetSubSets
{
    template <typename Key>
    class ClearableHashSet
        : public HashTable<Key, ClearableStringHashSetCell<Key>, StringHashTableHash, StringHashTableGrower<>, Allocator>
    {
    public:
        using Base = HashTable<Key, ClearableStringHashSetCell<Key>, StringHashTableHash, StringHashTableGrower<>, Allocator>;
        using typename Base::LookupResult;
        void clear()
        {
            ++this->version;
            this->m_size = 0;
        }
    };
    using T0 = StringHashTableEmpty<ClearableStringHashSetCell<StringRef>>;
    using T1 = ClearableHashSet<StringKey8>;
    using T2 = ClearableHashSet<StringKey16>;
    using T3 = ClearableHashSet<StringKey24>;
    using Ts = ClearableHashSet<StringRef>;
};

template <typename Allocator = HashTableAllocator>
class ClearableStringHashSet : public StringHashTable<ClearableStringHashSetSubSets<Allocator>>
{
public:
    using Key = StringRef;
    using Base = StringHashTable<ClearableStringHashSetSubSets<Allocator>>;
    using Self = ClearableStringHashSet;
    using LookupResult = typename Base::LookupResult;

    using Base::Base;

    void clear()
    {
        this->m0.clearHasZero();
        this->m1.clear();
        this->m2.clear();
        this->m3.clear();
        this->ms.clear();
    }
};
