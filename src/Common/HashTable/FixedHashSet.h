#pragma once

#include <Common/HashTable/FixedHashTable.h>

template <typename Key, typename Allocator = HashTableAllocator>
class FixedHashSet : public FixedHashTable<Key, FixedHashTableCell<Key>, FixedHashTableStoredSize<FixedHashTableCell<Key>>, Allocator>
{
public:
    using Cell = FixedHashTableCell<Key>;
    using Base = FixedHashTable<Key, Cell, FixedHashTableStoredSize<Cell>, Allocator>;
    using Self = FixedHashSet;

    void merge(const Self & rhs)
    {
        for (size_t i = 0; i < Base::BUFFER_SIZE; ++i)
            if (Base::buf[i].isZero(*this) && !rhs.buf[i].isZero(*this))
                new (&Base::buf[i]) Cell(rhs.buf[i]);
    }
};
