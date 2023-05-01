#pragma once

/// Packed versions HashMap, please keep in sync with HashMap.h

#include <Common/HashTable/HashMap.h>

/// A pair that does not initialize the elements, if not needed.
///
/// NOTE: makePairNoInit() is omitted for PackedPairNoInit since it is not
/// required for PackedHashMap (see mergeBlockWithPipe() for details)
template <typename First, typename Second>
struct PackedPairNoInit
{
    First first;
    Second second;

    PackedPairNoInit() {} /// NOLINT

    template <typename FirstValue>
    PackedPairNoInit(FirstValue && first_, NoInitTag)
        : first(std::forward<FirstValue>(first_))
    {
    }

    template <typename FirstValue, typename SecondValue>
    PackedPairNoInit(FirstValue && first_, SecondValue && second_)
        : first(std::forward<FirstValue>(first_))
        , second(std::forward<SecondValue>(second_))
    {
    }
} __attribute__((packed));

/// Packed HashMap - HashMap with structure without padding
///
/// Sometimes padding in structure can be crucial, consider the following
/// example <UInt64, UInt16> as <Key, Value> in this case the padding overhead
/// is 0.375, and this can be major in case of lots of keys.
///
/// Note, there is no need to provide PackedHashSet, since it cannot have padding.
template <
    typename Key,
    typename Mapped,
    typename Hash = DefaultHash<Key>,
    typename Grower = HashTableGrower<>,
    typename Allocator = HashTableAllocator>
using PackedHashMap = HashMapTable<Key, HashMapCell<Key, Mapped, Hash, HashTableNoState, PackedPairNoInit<Key, Mapped>>, Hash, Grower, Allocator>;
