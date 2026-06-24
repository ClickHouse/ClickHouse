#include <Common/BlockedBloomFilter8Hashes.h>
#include <Common/Exception.h>
#include <Common/TargetSpecific.h>
#include <Common/PODArray.h>
#include <Columns/IColumn.h>

#include <city.h>

#include <bit>
#include <cstring>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

BlockedBloomFilter8Hashes::BlockedBloomFilter8Hashes(size_t size_bytes, UInt64 seed_)
    : seed(seed_)
{
    if (size_bytes == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The size of blocked bloom filter cannot be zero");

    size_t raw_num_blocks = (size_bytes + BYTES_PER_BLOCK - 1) / BYTES_PER_BLOCK;
    num_blocks = std::bit_ceil(raw_num_blocks);
    log_num_blocks = std::countr_zero(num_blocks);
    blocks.resize(num_blocks);
}

UInt64 BlockedBloomFilter8Hashes::hashKey(const char * data, size_t len) const
{
    return CityHash_v1_0_2::CityHash64WithSeed(data, len, seed);
}

size_t BlockedBloomFilter8Hashes::blockIndex(UInt64 hash) const
{
    return static_cast<size_t>((static_cast<UInt64>(hash >> 32) * num_blocks) >> 32);
}

BlockedBloomFilter8Hashes::Block BlockedBloomFilter8Hashes::makeMask(UInt32 hash_low)
{
    Block mask;
    for (size_t i = 0; i < WORDS_PER_BLOCK; ++i)
    {
        UInt32 bit_pos = (hash_low * SALT[i]) >> 27;
        mask.words[i] = UInt32(1) << bit_pos;
    }
    return mask;
}

void BlockedBloomFilter8Hashes::addHash(UInt64 hash)
{
    size_t idx = blockIndex(hash);
    Block mask = makeMask(static_cast<UInt32>(hash));
    for (size_t i = 0; i < WORDS_PER_BLOCK; ++i)
        blocks[idx].words[i] |= mask.words[i];
}

bool BlockedBloomFilter8Hashes::findHash(UInt64 hash) const
{
    size_t idx = blockIndex(hash);
    Block mask = makeMask(static_cast<UInt32>(hash));
    for (size_t i = 0; i < WORDS_PER_BLOCK; ++i)
    {
        if ((blocks[idx].words[i] & mask.words[i]) != mask.words[i])
            return false;
    }
    return true;
}

void BlockedBloomFilter8Hashes::add(const char * data, size_t len) { addHash(hashKey(data, len)); }
bool BlockedBloomFilter8Hashes::find(const char * data, size_t len) const { return findHash(hashKey(data, len)); }


/// ============================================================================
/// Batch operations
/// ============================================================================

namespace
{

inline size_t computeBlockIndex8(UInt64 hash, size_t num_blocks_)
{
    return static_cast<size_t>((static_cast<UInt64>(hash >> 32) * num_blocks_) >> 32);
}

} /// anonymous namespace

DECLARE_DEFAULT_CODE(

static size_t findBatchScalar(
    const BlockedBloomFilter8Hashes::Block * blocks, size_t num_blocks,
    const UInt64 * hashes, size_t num_rows, UInt8 * result)
{
    static constexpr size_t PD = 8;

    for (size_t i = 0; i < std::min(num_rows, PD); ++i)
        __builtin_prefetch(&blocks[computeBlockIndex8(hashes[i], num_blocks)], 0, 1);

    size_t found_count = 0;
    for (size_t i = 0; i < num_rows; ++i)
    {
        if (i + PD < num_rows)
            __builtin_prefetch(&blocks[computeBlockIndex8(hashes[i + PD], num_blocks)], 0, 1);

        size_t idx = computeBlockIndex8(hashes[i], num_blocks);
        auto hash_low = static_cast<UInt32>(hashes[i]);
        const auto & block = blocks[idx];
        bool found = true;
        for (size_t j = 0; j < BlockedBloomFilter8Hashes::WORDS_PER_BLOCK; ++j)
        {
            UInt32 bit_pos = (hash_low * BlockedBloomFilter8Hashes::SALT[j]) >> 27;
            UInt32 mask_word = UInt32(1) << bit_pos;
            if ((block.words[j] & mask_word) != mask_word)
            {
                found = false;
                break;
            }
        }
        result[i] = found ? 1 : 0;
        found_count += found ? 1 : 0;
    }
    return found_count;
}

static void addBatchScalar(
    BlockedBloomFilter8Hashes::Block * blocks, size_t num_blocks,
    const UInt64 * hashes, size_t num_rows)
{
    static constexpr size_t PD = 8;

    for (size_t i = 0; i < std::min(num_rows, PD); ++i)
        __builtin_prefetch(&blocks[computeBlockIndex8(hashes[i], num_blocks)], 0, 3);

    for (size_t i = 0; i < num_rows; ++i)
    {
        if (i + PD < num_rows)
            __builtin_prefetch(&blocks[computeBlockIndex8(hashes[i + PD], num_blocks)], 0, 3);

        size_t idx = computeBlockIndex8(hashes[i], num_blocks);
        auto hash_low = static_cast<UInt32>(hashes[i]);
        for (size_t j = 0; j < BlockedBloomFilter8Hashes::WORDS_PER_BLOCK; ++j)
        {
            UInt32 bit_pos = (hash_low * BlockedBloomFilter8Hashes::SALT[j]) >> 27;
            blocks[idx].words[j] |= UInt32(1) << bit_pos;
        }
    }
}

) /// DECLARE_DEFAULT_CODE


#if USE_MULTITARGET_CODE

#include <immintrin.h>

DECLARE_X86_64_V3_SPECIFIC_CODE(

static size_t findBatchAVX2(
    const BlockedBloomFilter8Hashes::Block * blocks, size_t num_blocks,
    const UInt64 * hashes, size_t num_rows, UInt8 * result)
{
    static constexpr size_t PD = 8;

    const __m256i salts = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(BlockedBloomFilter8Hashes::SALT));
    const __m256i ones = _mm256_set1_epi32(1);
    const __m256i shift27 = _mm256_set1_epi32(27);

    for (size_t i = 0; i < std::min(num_rows, PD); ++i)
        __builtin_prefetch(&blocks[computeBlockIndex8(hashes[i], num_blocks)], 0, 1);

    size_t found_count = 0;
    for (size_t i = 0; i < num_rows; ++i)
    {
        if (i + PD < num_rows)
            __builtin_prefetch(&blocks[computeBlockIndex8(hashes[i + PD], num_blocks)], 0, 1);

        size_t idx = computeBlockIndex8(hashes[i], num_blocks);
        __m256i h = _mm256_set1_epi32(static_cast<int>(static_cast<UInt32>(hashes[i])));
        __m256i product = _mm256_mullo_epi32(h, salts);
        __m256i bit_pos = _mm256_srlv_epi32(product, shift27);
        __m256i mask = _mm256_sllv_epi32(ones, bit_pos);
        __m256i block_data = _mm256_load_si256(reinterpret_cast<const __m256i *>(&blocks[idx]));
        __m256i anded = _mm256_and_si256(block_data, mask);
        __m256i cmp = _mm256_cmpeq_epi32(anded, mask);
        bool found = _mm256_movemask_epi8(cmp) == -1;
        result[i] = found ? 1 : 0;
        found_count += found ? 1 : 0;
    }
    return found_count;
}

static void addBatchAVX2(
    BlockedBloomFilter8Hashes::Block * blocks, size_t num_blocks,
    const UInt64 * hashes, size_t num_rows)
{
    static constexpr size_t PD = 8;

    const __m256i salts = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(BlockedBloomFilter8Hashes::SALT));
    const __m256i ones = _mm256_set1_epi32(1);
    const __m256i shift27 = _mm256_set1_epi32(27);

    for (size_t i = 0; i < std::min(num_rows, PD); ++i)
        __builtin_prefetch(&blocks[computeBlockIndex8(hashes[i], num_blocks)], 0, 3);

    for (size_t i = 0; i < num_rows; ++i)
    {
        if (i + PD < num_rows)
            __builtin_prefetch(&blocks[computeBlockIndex8(hashes[i + PD], num_blocks)], 0, 3);

        size_t idx = computeBlockIndex8(hashes[i], num_blocks);
        __m256i h = _mm256_set1_epi32(static_cast<int>(static_cast<UInt32>(hashes[i])));
        __m256i product = _mm256_mullo_epi32(h, salts);
        __m256i bit_pos = _mm256_srlv_epi32(product, shift27);
        __m256i mask = _mm256_sllv_epi32(ones, bit_pos);
        __m256i block_data = _mm256_load_si256(reinterpret_cast<const __m256i *>(&blocks[idx]));
        block_data = _mm256_or_si256(block_data, mask);
        _mm256_store_si256(reinterpret_cast<__m256i *>(&blocks[idx]), block_data);
    }
}

) /// DECLARE_X86_64_V3_SPECIFIC_CODE

#endif /// USE_MULTITARGET_CODE


size_t BlockedBloomFilter8Hashes::findBatchImpl(const UInt64 * hashes, size_t num_rows, UInt8 * result) const
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v3))
        return TargetSpecific::x86_64_v3::findBatchAVX2(blocks.data(), num_blocks, hashes, num_rows, result);
#endif
    return TargetSpecific::Default::findBatchScalar(blocks.data(), num_blocks, hashes, num_rows, result);
}

void BlockedBloomFilter8Hashes::addBatchImpl(const UInt64 * hashes, size_t num_rows)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v3))
    {
        TargetSpecific::x86_64_v3::addBatchAVX2(blocks.data(), num_blocks, hashes, num_rows);
        return;
    }
#endif
    TargetSpecific::Default::addBatchScalar(blocks.data(), num_blocks, hashes, num_rows);
}


/// ============================================================================
/// Public batch methods
/// ============================================================================

void BlockedBloomFilter8Hashes::addBatch(const IColumn & column, size_t num_rows)
{
    if (num_rows == 0) return;
    PODArray<UInt64> hashes(num_rows);
    for (size_t i = 0; i < num_rows; ++i)
    {
        auto value = column.getDataAt(i);
        hashes[i] = hashKey(value.data(), value.size());
    }
    addBatchImpl(hashes.data(), num_rows);
}

size_t BlockedBloomFilter8Hashes::findBatch(const IColumn & column, size_t num_rows, UInt8 * result) const
{
    if (num_rows == 0) return 0;
    PODArray<UInt64> hashes(num_rows);
    for (size_t i = 0; i < num_rows; ++i)
    {
        auto value = column.getDataAt(i);
        hashes[i] = hashKey(value.data(), value.size());
    }
    return findBatchImpl(hashes.data(), num_rows, result);
}


/// ============================================================================
/// Merge and statistics
/// ============================================================================

void BlockedBloomFilter8Hashes::merge(const BlockedBloomFilter8Hashes & other)
{
    if (num_blocks != other.num_blocks)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot merge blocked bloom filters with different sizes: {} vs {}", num_blocks, other.num_blocks);

    auto * dst = reinterpret_cast<UInt32 *>(blocks.data());
    const auto * src = reinterpret_cast<const UInt32 *>(other.blocks.data());
    size_t total_words = num_blocks * WORDS_PER_BLOCK;
    for (size_t i = 0; i < total_words; ++i)
        dst[i] |= src[i];
}

size_t BlockedBloomFilter8Hashes::countSetBits() const
{
    size_t count = 0;
    const auto * p = reinterpret_cast<const UInt64 *>(blocks.data());
    size_t total_u64 = num_blocks * WORDS_PER_BLOCK / 2;
    for (size_t i = 0; i < total_u64; ++i)
        count += std::popcount(p[i]);
    return count;
}

size_t BlockedBloomFilter8Hashes::totalBits() const { return num_blocks * BYTES_PER_BLOCK * 8; }
size_t BlockedBloomFilter8Hashes::memoryUsageBytes() const { return blocks.capacity() * sizeof(Block); }

}
