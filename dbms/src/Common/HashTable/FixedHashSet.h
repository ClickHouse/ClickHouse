#pragma once

#include <Common/HashTable/FixedHashTable.h>

template <typename Key, typename Allocator = HashTableAllocator>
class FixedHashSet : public FixedHashTable<Key, FixedHashTableCell<Key>, Allocator>
{
public:
    using Cell = FixedHashTableCell<Key>;
    using Base = FixedHashTable<Key, Cell, Allocator>;
    using Self = FixedHashSet;

    void merge(const Self & rhs)
    {
        rhs.forEachCell([&](Key i, const Cell & cell)
        {
            if (Base::buf[i].isZero(*this))
                new (&Base::buf[i]) Cell(cell);
        });
    }

    /// NOTE: Currently this method isn't used. When it does, the ReadBuffer should
    ///  contain the Key explicitly.
    // void readAndMerge(DB::ReadBuffer & rb)
    // {

    // }
};
