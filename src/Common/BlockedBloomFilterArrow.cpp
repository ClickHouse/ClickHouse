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

#include <Common/BlockedBloomFilterArrow.h>
#include <Common/Exception.h>
#include <Common/PODArray.h>
#include <Common/randomSeed.h>
#include <Columns/IColumn.h>

#include <city.h>

#include <bit>
#include <cstring>
#include <random>
#include <pcg_random.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}


/// ============================================================================
/// Mask table generation (same algorithm as Arrow's BloomFilterMasks)
/// ============================================================================

/// Static instance — generated once at program start.
BlockedBloomFilterArrow::MaskTable BlockedBloomFilterArrow::mask_table;

/// Helper to get/set individual bits in the mask data array.
namespace
{

inline bool getBit(const UInt8 * buf, int pos)
{
    return (buf[pos / 8] >> (pos % 8)) & 1;
}

inline void setBit(UInt8 * buf, int pos)
{
    buf[pos / 8] |= (1 << (pos % 8));
}

} /// anonymous namespace

/// Sliding-window mask generation (same algorithm as Arrow's BloomFilterMasks).
///
/// Masks are stored as a single bit vector at 1-bit intervals: mask i starts at bit i
/// and spans 57 bits. Adjacent masks overlap by 56 bits, differing by only 1 bit.
/// This produces well-distributed masks with exactly 4-5 bits set per 57-bit window.
BlockedBloomFilterArrow::MaskTable::MaskTable()
{
    memset(data, 0, sizeof(data));

    pcg64_fast rng(randomSeed());
    std::uniform_int_distribution<UInt64> dist;
    auto random = [&](int min_val, int max_val)
    {
        return min_val + static_cast<int>(dist(rng) % (max_val - min_val + 1));
    };

    /// Step 1: Generate the first mask by picking random bit positions.
    static constexpr int MIN_BITS_SET = 4;
    static constexpr int MAX_BITS_SET = 5;

    int num_bits_set = random(MIN_BITS_SET, MAX_BITS_SET);
    for (int i = 0; i < num_bits_set; ++i)
    {
        for (;;)
        {
            int bit_pos = random(0, BITS_PER_MASK - 1);
            if (!getBit(data, bit_pos))
            {
                setBit(data, bit_pos);
                break;
            }
        }
    }

    /// Step 2: Slide the window one bit at a time, deciding each new bit.
    /// As the window moves right by 1, the leftmost bit of the previous mask "leaves"
    /// and we decide whether the new rightmost bit should be 0 or 1.
    int num_bits_total = NUM_MASKS + BITS_PER_MASK - 1;
    for (int i = BITS_PER_MASK; i < num_bits_total; ++i)
    {
        int bit_leaving = getBit(data, i - BITS_PER_MASK) ? 1 : 0;

        /// Must set to 1: losing a bit would drop below minimum.
        if (bit_leaving == 1 && num_bits_set == MIN_BITS_SET)
        {
            setBit(data, i);
            continue;
        }

        /// Must leave as 0: adding a bit would exceed maximum.
        if (bit_leaving == 0 && num_bits_set == MAX_BITS_SET)
            continue;

        /// Random: use expected density (4.5 / 57 ≈ 7.9%) as probability of 1.
        if (random(0, BITS_PER_MASK * 2 - 1) < MIN_BITS_SET + MAX_BITS_SET)
        {
            setBit(data, i);
            if (bit_leaving == 0)
                ++num_bits_set;
        }
        else
        {
            if (bit_leaving == 1)
                --num_bits_set;
        }
    }
}


/// ============================================================================
/// Core operations
/// ============================================================================

BlockedBloomFilterArrow::BlockedBloomFilterArrow(size_t size_bytes, UInt64 seed_)
    : seed(seed_)
{
    if (size_bytes == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The size of blocked bloom filter cannot be zero");

    /// Each block is 8 bytes (one uint64_t).
    size_t raw_num_blocks = (size_bytes + sizeof(UInt64) - 1) / sizeof(UInt64);
    num_blocks = std::bit_ceil(raw_num_blocks);
    block_mask = num_blocks - 1;
    blocks.resize(num_blocks, 0);
}

UInt64 BlockedBloomFilterArrow::hashKey(const char * data, size_t len) const
{
    return CityHash_v1_0_2::CityHash64WithSeed(data, len, seed);
}

UInt64 BlockedBloomFilterArrow::computeMask(UInt64 hash) const
{
    /// Low 10 bits select one of 1024 pre-generated masks.
    int mask_id = static_cast<int>(hash & (MaskTable::NUM_MASKS - 1));
    UInt64 mask = mask_table.get(mask_id);

    /// Next 6 bits select rotation amount (0-63).
    int rotation = (hash >> MaskTable::LOG_NUM_MASKS) & 63;
    return std::rotl(mask, rotation);
}

size_t BlockedBloomFilterArrow::blockIndex(UInt64 hash) const
{
    /// Use upper bits (after the 16 bits used for mask selection + rotation).
    return static_cast<size_t>((hash >> (MaskTable::LOG_NUM_MASKS + 6)) & block_mask);
}

void BlockedBloomFilterArrow::addHash(UInt64 hash)
{
    blocks[blockIndex(hash)] |= computeMask(hash);
}

bool BlockedBloomFilterArrow::findHash(UInt64 hash) const
{
    UInt64 m = computeMask(hash);
    return (blocks[blockIndex(hash)] & m) == m;
}

void BlockedBloomFilterArrow::add(const char * data, size_t len)
{
    addHash(hashKey(data, len));
}

bool BlockedBloomFilterArrow::find(const char * data, size_t len) const
{
    return findHash(hashKey(data, len));
}


/// ============================================================================
/// Batch operations with prefetching
/// ============================================================================

size_t BlockedBloomFilterArrow::findBatchImpl(const UInt64 * hashes, size_t num_rows, UInt8 * result) const
{
    static constexpr size_t PREFETCH_DISTANCE = 8;

    const UInt64 * block_data = blocks.data();

    /// Prefetch first batch of blocks.
    for (size_t i = 0; i < std::min(num_rows, PREFETCH_DISTANCE); ++i)
        __builtin_prefetch(&block_data[blockIndex(hashes[i])], 0, 1);

    size_t found_count = 0;
    for (size_t i = 0; i < num_rows; ++i)
    {
        if (i + PREFETCH_DISTANCE < num_rows)
            __builtin_prefetch(&block_data[blockIndex(hashes[i + PREFETCH_DISTANCE])], 0, 1);

        UInt64 m = computeMask(hashes[i]);
        bool found = (block_data[blockIndex(hashes[i])] & m) == m;
        result[i] = found ? 1 : 0;
        found_count += found ? 1 : 0;
    }
    return found_count;
}

void BlockedBloomFilterArrow::addBatchImpl(const UInt64 * hashes, size_t num_rows)
{
    static constexpr size_t PREFETCH_DISTANCE = 8;

    UInt64 * block_data = blocks.data();

    /// Prefetch first batch of blocks.
    for (size_t i = 0; i < std::min(num_rows, PREFETCH_DISTANCE); ++i)
        __builtin_prefetch(&block_data[blockIndex(hashes[i])], 0, 3);

    for (size_t i = 0; i < num_rows; ++i)
    {
        if (i + PREFETCH_DISTANCE < num_rows)
            __builtin_prefetch(&block_data[blockIndex(hashes[i + PREFETCH_DISTANCE])], 0, 3);

        block_data[blockIndex(hashes[i])] |= computeMask(hashes[i]);
    }
}


/// ============================================================================
/// Public batch methods
/// ============================================================================

void BlockedBloomFilterArrow::addBatch(const IColumn & column, size_t num_rows)
{
    if (num_rows == 0)
        return;

    /// Phase 1: Hash all rows into a temporary buffer.
    PODArray<UInt64> hashes(num_rows);
    for (size_t i = 0; i < num_rows; ++i)
    {
        auto value = column.getDataAt(i);
        hashes[i] = hashKey(value.data(), value.size());
    }

    /// Phase 2: Insert all hashes with prefetching.
    addBatchImpl(hashes.data(), num_rows);
}

size_t BlockedBloomFilterArrow::findBatch(const IColumn & column, size_t num_rows, UInt8 * result) const
{
    if (num_rows == 0)
        return 0;

    /// Phase 1: Hash all rows into a temporary buffer.
    PODArray<UInt64> hashes(num_rows);
    for (size_t i = 0; i < num_rows; ++i)
    {
        auto value = column.getDataAt(i);
        hashes[i] = hashKey(value.data(), value.size());
    }

    /// Phase 2: Probe all hashes with prefetching.
    return findBatchImpl(hashes.data(), num_rows, result);
}


/// ============================================================================
/// Merge and statistics
/// ============================================================================

void BlockedBloomFilterArrow::merge(const BlockedBloomFilterArrow & other)
{
    if (num_blocks != other.num_blocks)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot merge blocked bloom filters with different sizes: {} vs {}", num_blocks, other.num_blocks);

    for (size_t i = 0; i < num_blocks; ++i)
        blocks[i] |= other.blocks[i];
}

size_t BlockedBloomFilterArrow::countSetBits() const
{
    size_t count = 0;
    for (size_t i = 0; i < num_blocks; ++i)
        count += std::popcount(blocks[i]);
    return count;
}

size_t BlockedBloomFilterArrow::totalBits() const
{
    return num_blocks * 64;
}

size_t BlockedBloomFilterArrow::memoryUsageBytes() const
{
    return blocks.capacity() * sizeof(UInt64);
}

}
