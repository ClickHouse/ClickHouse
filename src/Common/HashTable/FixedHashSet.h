#pragma once

#include <Common/HashTable/FixedHashTable.h>

template <typename Key, typename Allocator = HashTableAllocator, size_t size_bits = sizeof(Key) * 8>
class FixedHashSet : public FixedHashTable<Key, FixedHashTableCell<Key>, FixedHashTableStoredSize<FixedHashTableCell<Key>>, Allocator, size_bits>
{
public:
    using Cell = FixedHashTableCell<Key>;
    using Base = FixedHashTable<Key, Cell, FixedHashTableStoredSize<Cell>, Allocator, size_bits>;
    using Self = FixedHashSet;

    void merge(const Self & rhs)
    {
        for (size_t i = 0; i < Base::BUFFER_SIZE; ++i)
            if (Base::buf[i].isZero(*this) && !rhs.buf[i].isZero(*this))
                new (&Base::buf[i]) Cell(rhs.buf[i]);
    }
};

/// Set counterpart of `FixedHashMapWithSizeBits`: the key range is narrowed to `size_bits` by the
/// caller, and the cell only records presence.
template <typename Key, size_t size_bits>
using FixedHashSetWithSizeBits = FixedHashSet<Key, HashTableAllocator, size_bits>;
