#include <Common/BlockedBloomFilter4Hashes.h>
#include <Common/Exception.h>
#include <Common/TargetSpecific.h>
#include <Common/PODArray.h>
#include <Columns/IColumn.h>

#include <city.h>

#include <bit>
#include <cstring>

#if defined(__aarch64__) && defined(__ARM_NEON)
#include <arm_neon.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

BlockedBloomFilter4Hashes::BlockedBloomFilter4Hashes(size_t size_bytes, UInt64 seed_)
    : seed(seed_)
{
    if (size_bytes == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The size of blocked bloom filter cannot be zero");

    size_t raw_num_blocks = (size_bytes + BYTES_PER_BLOCK - 1) / BYTES_PER_BLOCK;
    num_blocks = std::bit_ceil(raw_num_blocks);
    log_num_blocks = std::countr_zero(num_blocks);
    blocks.resize(num_blocks);
}

UInt64 BlockedBloomFilter4Hashes::hashKey(const char * data, size_t len) const
{
    return CityHash_v1_0_2::CityHash64WithSeed(data, len, seed);
}

size_t BlockedBloomFilter4Hashes::blockIndex(UInt64 hash) const
{
    return static_cast<size_t>((static_cast<UInt64>(hash >> 32) * num_blocks) >> 32);
}

BlockedBloomFilter4Hashes::Block BlockedBloomFilter4Hashes::makeMask(UInt32 hash_low)
{
    Block mask;
    for (size_t i = 0; i < WORDS_PER_BLOCK; ++i)
    {
        UInt32 bit_pos = (hash_low * SALT[i]) >> 27;
        mask.words[i] = UInt32(1) << bit_pos;
    }
    return mask;
}

void BlockedBloomFilter4Hashes::addHash(UInt64 hash)
{
    size_t idx = blockIndex(hash);
    Block mask = makeMask(static_cast<UInt32>(hash));
    for (size_t i = 0; i < WORDS_PER_BLOCK; ++i)
        blocks[idx].words[i] |= mask.words[i];
}

bool BlockedBloomFilter4Hashes::findHash(UInt64 hash) const
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

void BlockedBloomFilter4Hashes::add(const char * data, size_t len) { addHash(hashKey(data, len)); }
bool BlockedBloomFilter4Hashes::find(const char * data, size_t len) const { return findHash(hashKey(data, len)); }


/// ============================================================================
/// Batch operations
/// ============================================================================

namespace
{

inline size_t computeBlockIndex4(UInt64 hash, size_t num_blocks_)
{
    return static_cast<size_t>((static_cast<UInt64>(hash >> 32) * num_blocks_) >> 32);
}

} /// anonymous namespace


/// Scalar fallback (not needed on aarch64 where NEON is always available)
#if !defined(__aarch64__) || !defined(__ARM_NEON)
DECLARE_DEFAULT_CODE(

static size_t findBatchScalar(
    const BlockedBloomFilter4Hashes::Block * blocks, size_t num_blocks,
    const UInt64 * hashes, size_t num_rows, UInt8 * result)
{
    static constexpr size_t PD = 8;

    for (size_t i = 0; i < std::min(num_rows, PD); ++i)
        __builtin_prefetch(&blocks[computeBlockIndex4(hashes[i], num_blocks)], 0, 1);

    size_t found_count = 0;
    for (size_t i = 0; i < num_rows; ++i)
    {
        if (i + PD < num_rows)
            __builtin_prefetch(&blocks[computeBlockIndex4(hashes[i + PD], num_blocks)], 0, 1);

        size_t idx = computeBlockIndex4(hashes[i], num_blocks);
        auto hash_low = static_cast<UInt32>(hashes[i]);
        const auto & block = blocks[idx];
        bool found = true;
        for (size_t j = 0; j < BlockedBloomFilter4Hashes::WORDS_PER_BLOCK; ++j)
        {
            UInt32 bit_pos = (hash_low * BlockedBloomFilter4Hashes::SALT[j]) >> 27;
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
    BlockedBloomFilter4Hashes::Block * blocks, size_t num_blocks,
    const UInt64 * hashes, size_t num_rows)
{
    static constexpr size_t PD = 8;

    for (size_t i = 0; i < std::min(num_rows, PD); ++i)
        __builtin_prefetch(&blocks[computeBlockIndex4(hashes[i], num_blocks)], 0, 3);

    for (size_t i = 0; i < num_rows; ++i)
    {
        if (i + PD < num_rows)
            __builtin_prefetch(&blocks[computeBlockIndex4(hashes[i + PD], num_blocks)], 0, 3);

        size_t idx = computeBlockIndex4(hashes[i], num_blocks);
        auto hash_low = static_cast<UInt32>(hashes[i]);
        for (size_t j = 0; j < BlockedBloomFilter4Hashes::WORDS_PER_BLOCK; ++j)
        {
            UInt32 bit_pos = (hash_low * BlockedBloomFilter4Hashes::SALT[j]) >> 27;
            blocks[idx].words[j] |= UInt32(1) << bit_pos;
        }
    }
}

) /// DECLARE_DEFAULT_CODE
#endif /// !__aarch64__ || !__ARM_NEON


/// ARM NEON (aarch64)
#if defined(__aarch64__) && defined(__ARM_NEON)

static size_t findBatchNEON(
    const BlockedBloomFilter4Hashes::Block * blocks, size_t num_blocks,
    const UInt64 * hashes, size_t num_rows, UInt8 * result)
{
    static constexpr size_t PD = 8;
    const uint32x4_t salts = vld1q_u32(BlockedBloomFilter4Hashes::SALT);
    const uint32x4_t ones = vdupq_n_u32(1);

    for (size_t i = 0; i < std::min(num_rows, PD); ++i)
        __builtin_prefetch(&blocks[computeBlockIndex4(hashes[i], num_blocks)], 0, 1);

    size_t found_count = 0;
    for (size_t i = 0; i < num_rows; ++i)
    {
        if (i + PD < num_rows)
            __builtin_prefetch(&blocks[computeBlockIndex4(hashes[i + PD], num_blocks)], 0, 1);

        size_t idx = computeBlockIndex4(hashes[i], num_blocks);
        uint32x4_t h = vdupq_n_u32(static_cast<UInt32>(hashes[i]));
        uint32x4_t product = vmulq_u32(h, salts);
        uint32x4_t bit_pos = vshrq_n_u32(product, 27);
        uint32x4_t mask = vshlq_u32(ones, vreinterpretq_s32_u32(bit_pos));
        uint32x4_t block_data = vld1q_u32(blocks[idx].words);
        uint32x4_t anded = vandq_u32(block_data, mask);
        uint32x4_t cmp = vceqq_u32(anded, mask);
        bool found = vminvq_u32(cmp) != 0;
        result[i] = found ? 1 : 0;
        found_count += found ? 1 : 0;
    }
    return found_count;
}

static void addBatchNEON(
    BlockedBloomFilter4Hashes::Block * blocks, size_t num_blocks,
    const UInt64 * hashes, size_t num_rows)
{
    static constexpr size_t PD = 8;
    const uint32x4_t salts = vld1q_u32(BlockedBloomFilter4Hashes::SALT);
    const uint32x4_t ones = vdupq_n_u32(1);

    for (size_t i = 0; i < std::min(num_rows, PD); ++i)
        __builtin_prefetch(&blocks[computeBlockIndex4(hashes[i], num_blocks)], 0, 3);

    for (size_t i = 0; i < num_rows; ++i)
    {
        if (i + PD < num_rows)
            __builtin_prefetch(&blocks[computeBlockIndex4(hashes[i + PD], num_blocks)], 0, 3);

        size_t idx = computeBlockIndex4(hashes[i], num_blocks);
        uint32x4_t h = vdupq_n_u32(static_cast<UInt32>(hashes[i]));
        uint32x4_t product = vmulq_u32(h, salts);
        uint32x4_t bit_pos = vshrq_n_u32(product, 27);
        uint32x4_t mask = vshlq_u32(ones, vreinterpretq_s32_u32(bit_pos));
        uint32x4_t block_data = vld1q_u32(blocks[idx].words);
        block_data = vorrq_u32(block_data, mask);
        vst1q_u32(blocks[idx].words, block_data);
    }
}

#endif /// __aarch64__ && __ARM_NEON


/// x86 AVX2 (uses __m128i with AVX2 variable-shift instructions)
#if USE_MULTITARGET_CODE

#include <immintrin.h>

DECLARE_X86_64_V3_SPECIFIC_CODE(

static size_t findBatchSSE(
    const BlockedBloomFilter4Hashes::Block * blocks, size_t num_blocks,
    const UInt64 * hashes, size_t num_rows, UInt8 * result)
{
    static constexpr size_t PD = 8;
    const __m128i salts = _mm_loadu_si128(reinterpret_cast<const __m128i *>(BlockedBloomFilter4Hashes::SALT));
    const __m128i ones = _mm_set1_epi32(1);
    const __m128i shift27 = _mm_set1_epi32(27);

    for (size_t i = 0; i < std::min(num_rows, PD); ++i)
        __builtin_prefetch(&blocks[computeBlockIndex4(hashes[i], num_blocks)], 0, 1);

    size_t found_count = 0;
    for (size_t i = 0; i < num_rows; ++i)
    {
        if (i + PD < num_rows)
            __builtin_prefetch(&blocks[computeBlockIndex4(hashes[i + PD], num_blocks)], 0, 1);

        size_t idx = computeBlockIndex4(hashes[i], num_blocks);
        __m128i h = _mm_set1_epi32(static_cast<int>(static_cast<UInt32>(hashes[i])));
        __m128i product = _mm_mullo_epi32(h, salts);
        __m128i bit_pos = _mm_srlv_epi32(product, shift27);
        __m128i mask = _mm_sllv_epi32(ones, bit_pos);
        __m128i block_data = _mm_load_si128(reinterpret_cast<const __m128i *>(&blocks[idx]));
        __m128i anded = _mm_and_si128(block_data, mask);
        __m128i cmp = _mm_cmpeq_epi32(anded, mask);
        bool found = _mm_movemask_epi8(cmp) == 0xFFFF;
        result[i] = found ? 1 : 0;
        found_count += found ? 1 : 0;
    }
    return found_count;
}

static void addBatchSSE(
    BlockedBloomFilter4Hashes::Block * blocks, size_t num_blocks,
    const UInt64 * hashes, size_t num_rows)
{
    static constexpr size_t PD = 8;
    const __m128i salts = _mm_loadu_si128(reinterpret_cast<const __m128i *>(BlockedBloomFilter4Hashes::SALT));
    const __m128i ones = _mm_set1_epi32(1);
    const __m128i shift27 = _mm_set1_epi32(27);

    for (size_t i = 0; i < std::min(num_rows, PD); ++i)
        __builtin_prefetch(&blocks[computeBlockIndex4(hashes[i], num_blocks)], 0, 3);

    for (size_t i = 0; i < num_rows; ++i)
    {
        if (i + PD < num_rows)
            __builtin_prefetch(&blocks[computeBlockIndex4(hashes[i + PD], num_blocks)], 0, 3);

        size_t idx = computeBlockIndex4(hashes[i], num_blocks);
        __m128i h = _mm_set1_epi32(static_cast<int>(static_cast<UInt32>(hashes[i])));
        __m128i product = _mm_mullo_epi32(h, salts);
        __m128i bit_pos = _mm_srlv_epi32(product, shift27);
        __m128i mask = _mm_sllv_epi32(ones, bit_pos);
        __m128i block_data = _mm_load_si128(reinterpret_cast<const __m128i *>(&blocks[idx]));
        block_data = _mm_or_si128(block_data, mask);
        _mm_store_si128(reinterpret_cast<__m128i *>(&blocks[idx]), block_data);
    }
}

) /// DECLARE_X86_64_V3_SPECIFIC_CODE

#endif /// USE_MULTITARGET_CODE


size_t BlockedBloomFilter4Hashes::findBatchImpl(const UInt64 * hashes, size_t num_rows, UInt8 * result) const
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v3))
        return TargetSpecific::x86_64_v3::findBatchSSE(blocks.data(), num_blocks, hashes, num_rows, result);
#endif
#if defined(__aarch64__) && defined(__ARM_NEON)
    return findBatchNEON(blocks.data(), num_blocks, hashes, num_rows, result);
#else
    return TargetSpecific::Default::findBatchScalar(blocks.data(), num_blocks, hashes, num_rows, result);
#endif
}

void BlockedBloomFilter4Hashes::addBatchImpl(const UInt64 * hashes, size_t num_rows)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v3))
    {
        TargetSpecific::x86_64_v3::addBatchSSE(blocks.data(), num_blocks, hashes, num_rows);
        return;
    }
#endif
#if defined(__aarch64__) && defined(__ARM_NEON)
    addBatchNEON(blocks.data(), num_blocks, hashes, num_rows);
#else
    TargetSpecific::Default::addBatchScalar(blocks.data(), num_blocks, hashes, num_rows);
#endif
}


/// ============================================================================
/// Public batch methods
/// ============================================================================

void BlockedBloomFilter4Hashes::addBatch(const IColumn & column, size_t num_rows)
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

size_t BlockedBloomFilter4Hashes::findBatch(const IColumn & column, size_t num_rows, UInt8 * result) const
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

void BlockedBloomFilter4Hashes::merge(const BlockedBloomFilter4Hashes & other)
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

size_t BlockedBloomFilter4Hashes::countSetBits() const
{
    size_t count = 0;
    const auto * p = reinterpret_cast<const UInt64 *>(blocks.data());
    size_t total_u64 = num_blocks * WORDS_PER_BLOCK / 2;
    for (size_t i = 0; i < total_u64; ++i)
        count += std::popcount(p[i]);
    return count;
}

size_t BlockedBloomFilter4Hashes::totalBits() const { return num_blocks * BYTES_PER_BLOCK * 8; }
size_t BlockedBloomFilter4Hashes::memoryUsageBytes() const { return blocks.capacity() * sizeof(Block); }

}
