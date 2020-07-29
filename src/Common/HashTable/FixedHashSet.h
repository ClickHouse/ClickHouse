#pragma once

#include <Common/HashTable/FixedHashTable.h>

template <typename Key>
class FixedHashSet : public FixedHashTable<Key, FixedHashTableCell<Key>, FixedHashTableStoredSize<FixedHashTableCell<Key>>>
{
public:
    using Cell = FixedHashTableCell<Key>;
    using Base = FixedHashTable<Key, Cell, FixedHashTableStoredSize<Cell>>;
    using Self = FixedHashSet;

    void merge(const Self & rhs)
    {
        for (size_t i = 0; i < Base::BUFFER_SIZE; ++i)
            if (Base::buf[i].isZero(*this) && !rhs.buf[i].isZero(*this))
                new (&Base::buf[i]) Cell(rhs.buf[i]);
    }

    /// NOTE: Currently this method isn't used. When it does, the ReadBuffer should
    ///  contain the Key explicitly.
    // void readAndMerge(DB::ReadBuffer & rb)
    // {

    // }
};
