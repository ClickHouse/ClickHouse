#pragma once

/// Variant 2: 4-hash salt-multiply, 16-byte blocks, SSE/NEON SIMD.
/// Each block is 4 × uint32_t = 16 bytes. 4 salt multipliers generate one bit per word.
/// Natural fit for 128-bit SIMD: SSE (x86 with AVX2 variable shifts) and NEON (ARM).
///
/// Based on the blocked bloom filter from "Cache-, Hash- and Space-Efficient
/// Bloom Filters" (Putze, Sanders, Singler, 2007). Salt-multiply variant
/// also used by Apache Impala, Apache Doris and other systems.

#include <base/types.h>
#include <Columns/IColumn.h>

#include <bit>
#include <cstring>
#include <vector>


namespace DB
{

class BlockedBloomFilter4Hashes
{
public:
    static constexpr size_t WORDS_PER_BLOCK = 4;
    static constexpr size_t BYTES_PER_BLOCK = WORDS_PER_BLOCK * sizeof(UInt32); /// 16

    BlockedBloomFilter4Hashes(size_t size_bytes, UInt64 seed_);

    void add(const char * data, size_t len);
    bool find(const char * data, size_t len) const;

    void addBatch(const IColumn & column, size_t num_rows);
    size_t findBatch(const IColumn & column, size_t num_rows, UInt8 * result) const;

    void merge(const BlockedBloomFilter4Hashes & other);
    size_t countSetBits() const;
    size_t totalBits() const;
    size_t memoryUsageBytes() const;

    struct alignas(BYTES_PER_BLOCK) Block
    {
        UInt32 words[WORDS_PER_BLOCK] = {};
    };

    static_assert(sizeof(Block) == BYTES_PER_BLOCK);

    static constexpr UInt32 SALT[WORDS_PER_BLOCK] = {
        0x47b6137bU, 0x44974d91U, 0x8824ad5bU, 0xa2b7289dU,
    };

private:
    UInt64 hashKey(const char * data, size_t len) const;
    size_t blockIndex(UInt64 hash) const;
    static Block makeMask(UInt32 hash_low);
    void addHash(UInt64 hash);
    bool findHash(UInt64 hash) const;
    size_t findBatchImpl(const UInt64 * hashes, size_t num_rows, UInt8 * result) const;
    void addBatchImpl(const UInt64 * hashes, size_t num_rows);

    UInt64 seed;
    size_t num_blocks;
    size_t log_num_blocks;
    std::vector<Block> blocks;
};

}
