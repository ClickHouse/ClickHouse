#pragma once

#include <Common/HashTable/FixedHashTable.h>

template <typename Key, typename Allocator = HashTableAllocator>
class FixedHashSet : public FixedHashTable<Key, FixedHashTableCell<Key>, Allocator>
{
public:
    using Base = FixedHashTable<Key, FixedHashTableCell<Key>, Allocator>;
    using Self = FixedHashSet;

    void merge(const Self & rhs)
    {
        for (size_t i = 0; i < Base::BUFFER_SIZE; ++i)
            if (Base::buf[i].isZero(*this) && !rhs.buf[i].isZero(*this))
                Base::buf[i] = rhs.buf[i];
    }

    /// NOTE: Currently this method isn't used. When it does, the ReadBuffer should
    ///  contain the Key explicitly.
    // void readAndMerge(DB::ReadBuffer & rb)
    // {

    // }
};
