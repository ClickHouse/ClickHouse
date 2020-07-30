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

    const VoidKey getKey() const { return {}; }
    VoidMapped getMapped() const { return {}; }

    bool isZero(const State & state) const { return version != state.version; }
    void setZero() { version = 0; }

    struct CellExt
    {
        Key key;
        const VoidKey getKey() const { return {}; }
        VoidMapped getMapped() const { return {}; }
        const value_type & getValue() const { return key; }
        void update(Key && key_, FixedClearableHashTableCell *) { key = key_; }
    };
};


template <typename Key, typename Allocator = HashTableAllocator>
class FixedClearableHashSet : public FixedHashTable<
    Key, FixedClearableHashTableCell<Key>, FixedHashTableStoredSize<FixedClearableHashTableCell<Key>>, Allocator>
{
public:
    using Base = FixedHashTable<Key, FixedClearableHashTableCell<Key>, FixedHashTableStoredSize<FixedClearableHashTableCell<Key>>, Allocator>;
    using LookupResult = typename Base::LookupResult;

    void clear()
    {
        ++this->version;
        this->m_size = 0;
    }
};
