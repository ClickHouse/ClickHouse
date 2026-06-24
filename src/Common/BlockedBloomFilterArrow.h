#pragma once

/// Algorithm based on Arrow's acero BlockedBloomFilter.
///
/// Licensed to the Apache Software Foundation (ASF) under one
/// or more contributor license agreements. See the NOTICE file
/// distributed with this work for additional information
/// regarding copyright ownership. The ASF licenses this file
/// to you under the Apache License, Version 2.0 (the
/// "License"); you may not use this file except in compliance
/// with the License. You may obtain a copy of the License at
///
///   http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing,
/// software distributed under the License is distributed on an
/// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
/// KIND, either express or implied. See the License for the
/// specific language governing permissions and limitations
/// under the License.

#include <base/types.h>
#include <Columns/IColumn.h>

#include <bit>
#include <cstring>
#include <vector>


namespace DB
{

/// A cache-friendly blocked bloom filter where each block is a single uint64_t
/// and bits are selected via a pre-generated mask table with rotation.
///
/// Algorithm (based on Arrow's acero BlockedBloomFilterArrow):
/// - 1024 pre-generated 57-bit masks, each with 4-5 bits set.
/// - For each key hash:
///   - Low 10 bits select one of 1024 masks.
///   - Next 6 bits select a rotation amount (0-63).
///   - Remaining upper bits select a 64-bit block.
///   - The rotated mask is OR'd (insert) or AND-tested (probe) with the block.
///
/// Each probe touches only one 8-byte block plus the mask table (~136 bytes,
/// hot in L1 cache). No SIMD required — the scalar path is a table lookup,
/// a rotate, and an AND.
///
/// Used only for in-memory runtime JOIN filters, never serialized to disk.
class BlockedBloomFilterArrow
{
public:
    /// Construct a filter with the given total size in bytes and hash seed.
    /// The number of blocks is rounded up to the nearest power of 2.
    BlockedBloomFilterArrow(size_t size_bytes, UInt64 seed_);

    /// Insert a single key.
    void add(const char * data, size_t len);

    /// Probe a single key. Returns true if the key might be present, false if definitely absent.
    bool find(const char * data, size_t len) const;

    /// Batch insert: hash all rows from column, then insert all hashes.
    void addBatch(const IColumn & column, size_t num_rows);

    /// Batch probe: hash all rows, probe with prefetching. Writes 1/0 into result array.
    /// Returns the number of keys found (for statistics).
    size_t findBatch(const IColumn & column, size_t num_rows, UInt8 * result) const;

    /// Merge another filter into this one via bitwise OR.
    /// Both filters must have the same number of blocks.
    void merge(const BlockedBloomFilterArrow & other);

    /// Count the number of set bits (for worthiness check).
    size_t countSetBits() const;

    /// Total number of bits in the filter.
    size_t totalBits() const;

    /// Memory usage in bytes.
    size_t memoryUsageBytes() const;

private:
    /// Pre-generated mask table. 1024 masks stored as a bit vector.
    /// Each mask is 57 bits with 4-5 bits set. Masks are accessed at
    /// arbitrary bit offsets via unaligned 64-bit loads.
    struct MaskTable
    {
        static constexpr int LOG_NUM_MASKS = 10;
        static constexpr int NUM_MASKS = 1 << LOG_NUM_MASKS;
        static constexpr int BITS_PER_MASK = 57;
        static constexpr UInt64 FULL_MASK = (1ULL << BITS_PER_MASK) - 1;
        static constexpr int TOTAL_BYTES = (NUM_MASKS + 64) / 8;

        UInt8 data[TOTAL_BYTES];

        MaskTable();

        inline UInt64 get(int index) const
        {
            UInt64 raw;
            memcpy(&raw, data + index / 8, sizeof(UInt64));
            return (raw >> (index % 8)) & FULL_MASK;
        }
    };

    static MaskTable mask_table;

    /// Compute the hash of a key.
    UInt64 hashKey(const char * data, size_t len) const;

    /// Given a hash, compute the mask (table lookup + rotate).
    UInt64 computeMask(UInt64 hash) const;

    /// Given a hash, return the block index.
    size_t blockIndex(UInt64 hash) const;

    /// Insert a pre-computed hash.
    void addHash(UInt64 hash);

    /// Probe a pre-computed hash.
    bool findHash(UInt64 hash) const;

    /// Batch implementations.
    size_t findBatchImpl(const UInt64 * hashes, size_t num_rows, UInt8 * result) const;
    void addBatchImpl(const UInt64 * hashes, size_t num_rows);

    UInt64 seed;
    size_t num_blocks; /// Always a power of 2.
    size_t block_mask; /// num_blocks - 1, for fast modulo.
    std::vector<UInt64> blocks;
};

}
