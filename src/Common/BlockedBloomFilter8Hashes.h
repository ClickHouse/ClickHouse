#pragma once

/// Variant 1: 8-hash salt-multiply, 32-byte blocks, AVX2 SIMD.
/// Each block is 8 × uint32_t = 32 bytes. 8 salt multipliers generate one bit per word.
/// AVX2 path processes all 8 words in a single __m256i operation.
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

class BlockedBloomFilter8Hashes
{
public:
    static constexpr size_t WORDS_PER_BLOCK = 8;
    static constexpr size_t BYTES_PER_BLOCK = WORDS_PER_BLOCK * sizeof(UInt32); /// 32

    BlockedBloomFilter8Hashes(size_t size_bytes, UInt64 seed_);

    void add(const char * data, size_t len);
    bool find(const char * data, size_t len) const;

    void addBatch(const IColumn & column, size_t num_rows);
    size_t findBatch(const IColumn & column, size_t num_rows, UInt8 * result) const;

    void merge(const BlockedBloomFilter8Hashes & other);
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
        0x705495c7U, 0x2df1424bU, 0x9efc4947U, 0x5c6bfb31U,
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
