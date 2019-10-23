#pragma once

#include <Common/HashTable/ClearableHashSet.h>
#include <Common/HashTable/FixedHashTable.h>

template <typename Key>
struct FixedClearableHashTableCell
{
    using State = ClearableHashSetState;

    using value_type = Key;
    using mapped_type = VoidMapped;
    UInt32 version;

    FixedClearableHashTableCell() {}
    FixedClearableHashTableCell(const Key &, const State & state) : version(state.version) {}

    const VoidKey getKey() const { return VoidKey{}; }
    VoidMapped & getMapped() const { return voidMapped; }

    bool isZero(const State & state) const { return version != state.version; }
    void setZero() { version = 0; }
};


template <typename Key, typename Allocator = HashTableAllocator>
class FixedClearableHashSet : public FixedHashTable<Key, FixedClearableHashTableCell<Key>, Allocator>
{
public:
    using Base = FixedHashTable<Key, FixedClearableHashTableCell<Key>, Allocator>;
    using key_type = Key;
    using value_type = typename FixedClearableHashSet::cell_type::value_type;
    using LookupResult = typename Base::LookupResult;

    void clear()
    {
        ++this->version;
        this->m_size = 0;
    }
};
