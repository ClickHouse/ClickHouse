#pragma once

#include <Common/HashTable/FixedHashMap.h>
#include <Common/HashTable/PartitionedFixedHashTable.h>
#include <Common/HashTable/TwoLevelHashTable.h>


/// A `FixedHashMap` whose keys are split into buckets for a fill from several threads. See `PartitionedFixedHashTable`.
template <typename Key, typename Mapped, size_t size_bits = sizeof(Key) * 8, size_t BITS_FOR_BUCKET = DEFAULT_BITS_FOR_BUCKET>
using PartitionedFixedHashMap = PartitionedFixedHashTable<FixedHashMapWithSizeBits<Key, Mapped, size_bits>, BITS_FOR_BUCKET>;
