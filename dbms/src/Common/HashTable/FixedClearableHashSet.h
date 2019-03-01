#pragma once

#include <Common/HashTable/ClearableHashSet.h>
#include <Common/HashTable/FixedHashTable.h>


template <typename Key>
struct FixedClearableHashTableCell
{
    using State = ClearableHashSetState;

    using value_type = Key;
    UInt32 version;

    FixedClearableHashTableCell() {}
    FixedClearableHashTableCell(const Key &, const State & state) : version(state.version) {}

    bool isZero(const State & state) const { return version != state.version; }
    void setZero() { version = 0; }
    static constexpr bool need_zero_value_storage = false;
    void setMapped(const value_type & /*value*/) {}

    struct CellExt
    {
        Key key;
        value_type & getValueMutable() { return key; }
        const value_type & getValue() const { return key; }
        void update(Key && key_, FixedClearableHashTableCell *) { key = key_; }
    };
};


template <typename Key, typename Allocator = HashTableAllocator>
class FixedClearableHashSet : public FixedHashTable<Key, FixedClearableHashTableCell<Key>, Allocator>
{
public:
    using key_type = Key;
    using value_type = typename FixedClearableHashSet::cell_type::value_type;

    void clear()
    {
        ++this->version;
        this->m_size = 0;
    }
};
