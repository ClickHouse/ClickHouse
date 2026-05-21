/// Multi-buffer MD5 function.
///
/// Processes multiple independent MD5 digests in parallel by interleaving
/// their round computations. On x86-64 with AVX2 or AVX-512, uses SIMD to
/// compute 16 or 32 digests simultaneously. On other architectures, uses a
/// scalar multi-buffer approach with 4 independent dependency chains.

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/PerformanceAdaptors.h>
#include <base/IPv4andIPv6.h>
#include <base/unaligned.h>

#include <Common/TargetSpecific.h>

#include "config.h"

#if USE_SSL
#    include <Common/Crypto/OpenSSLInitializer.h>
#endif

#if USE_MULTITARGET_CODE && (defined(__x86_64__) || defined(_M_X64))
#include <immintrin.h>
#endif

#include <algorithm>
#include <cstdint>
#include <cstring>

namespace
{

/// MD5 initial state (RFC 1321).
constexpr uint32_t MD5_A0 = 0x67452301;
constexpr uint32_t MD5_B0 = 0xefcdab89;
constexpr uint32_t MD5_C0 = 0x98badcfe;
constexpr uint32_t MD5_D0 = 0x10325476;

constexpr size_t MD5_DIGEST_LEN = 16;

/// Placeholder for unused SIMD lanes. Only passed with length=0, so zero bytes are read from it.
constexpr uint8_t md5_dummy_lane_byte = 0;

/// Pad a message per RFC 1321. Writes the final 1-2 blocks into `out`.
/// Returns the number of final blocks written (1 or 2).
size_t md5PadFinalBlocks(const uint8_t * data, size_t len, uint8_t * out)
{
    size_t full_blocks = len / 64;
    size_t tail = len % 64;
    size_t num_blocks = (len + 9 + 63) / 64;
    size_t final_count = num_blocks - full_blocks;

    std::memset(out, 0, final_count * 64);
    std::memcpy(out, data + full_blocks * 64, tail);
    out[tail] = 0x80;

    uint64_t bit_len = static_cast<uint64_t>(len) * 8;
    unalignedStoreLittleEndian<uint64_t>(out + final_count * 64 - 8, bit_len);

    return final_count;
}

size_t numMD5Blocks(size_t len)
{
    return (len + 9 + 63) / 64;
}

} // anonymous namespace


namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int SUPPORT_IS_DISABLED;
}

/// Shared base class: common IFunction overrides.
class FunctionMD5Base : public IFunction
{
public:
    static constexpr auto name = "MD5";

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    bool canThrow(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    /// Disable default Variant implementation for compatibility.
    /// Hash values must remain stable, so we don't want the Variant adaptor to change hash computation.
    bool useDefaultImplementationForVariant() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isStringOrFixedString(arguments[0]) && !isIPv6(arguments[0])) [[unlikely]]
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", arguments[0]->getName(), getName());
        return std::make_shared<DataTypeFixedString>(MD5_DIGEST_LEN);
    }
};


/// ============================================================
/// Multi-buffer MD5, templated on Ops.
///
/// Template functions go inside DECLARE_MULTITARGET_CODE so each
/// target-specific copy is compiled with the correct ISA flags.
/// The Ops structs (ScalarMD5Ops, AVX2MD5Ops, AVX512MD5Ops) are
/// in their own DECLARE blocks and found via same-namespace lookup.
/// ============================================================

/// One interleaved MD5 round step for two independent groups.
/// Defined outside DECLARE_MULTITARGET_CODE because preprocessor
/// directives inside macro arguments are undefined behavior.
#define MD5_STEP_X2(func, w1, x1, y1, z1, w2, x2, y2, z2, g, s, ti) \
    { \
        Vec t1 = func(x1, y1, z1); \
        Vec t2 = func(x2, y2, z2); \
        t1 = Ops::add(t1, w1); \
        t2 = Ops::add(t2, w2); \
        Vec k = Ops::set1(ti); \
        t1 = Ops::add(t1, k); \
        t2 = Ops::add(t2, k); \
        t1 = Ops::add(t1, msg1[g]); \
        t2 = Ops::add(t2, msg2[g]); \
        (w1) = Ops::add(x1, Ops::template rotl<s>(t1)); \
        (w2) = Ops::add(x2, Ops::template rotl<s>(t2)); \
    }

DECLARE_MULTITARGET_CODE(

    template <typename Ops>
    struct MD5X2State
    {
        typename Ops::Vec a1, b1, c1, d1, a2, b2, c2, d2;
    };

    /// Process one 64-byte block for two independent groups of N lanes.
    /// The two groups are interleaved to let the CPU overlap independent chains.
    /// State is passed by value so the compiler keeps everything in registers.
    template <typename Ops>
    inline MD5X2State<Ops> md5MultiBufferBlockX2(
        typename Ops::Vec a1,
        typename Ops::Vec b1,
        typename Ops::Vec c1,
        typename Ops::Vec d1,
        typename Ops::Vec a2,
        typename Ops::Vec b2,
        typename Ops::Vec c2,
        typename Ops::Vec d2,
        const typename Ops::Vec msg1[16],
        const typename Ops::Vec msg2[16])
    {
        using Vec = typename Ops::Vec;
        Vec aa1 = a1;
        Vec bb1 = b1;
        Vec cc1 = c1;
        Vec dd1 = d1;
        Vec aa2 = a2;
        Vec bb2 = b2;
        Vec cc2 = c2;
        Vec dd2 = d2;

        // As in https://github.com/intel/isa-l_crypto/blob/ad5aab170d038340a81d973add670466e258f606/md5_mb/md5_ref.c
        // Round 1: F, shifts 7/12/17/22
        MD5_STEP_X2(Ops::F, a1, b1, c1, d1, a2, b2, c2, d2, 0, 7, 0xd76aa478)
        MD5_STEP_X2(Ops::F, d1, a1, b1, c1, d2, a2, b2, c2, 1, 12, 0xe8c7b756)
        MD5_STEP_X2(Ops::F, c1, d1, a1, b1, c2, d2, a2, b2, 2, 17, 0x242070db)
        MD5_STEP_X2(Ops::F, b1, c1, d1, a1, b2, c2, d2, a2, 3, 22, 0xc1bdceee)
        MD5_STEP_X2(Ops::F, a1, b1, c1, d1, a2, b2, c2, d2, 4, 7, 0xf57c0faf)
        MD5_STEP_X2(Ops::F, d1, a1, b1, c1, d2, a2, b2, c2, 5, 12, 0x4787c62a)
        MD5_STEP_X2(Ops::F, c1, d1, a1, b1, c2, d2, a2, b2, 6, 17, 0xa8304613)
        MD5_STEP_X2(Ops::F, b1, c1, d1, a1, b2, c2, d2, a2, 7, 22, 0xfd469501)
        MD5_STEP_X2(Ops::F, a1, b1, c1, d1, a2, b2, c2, d2, 8, 7, 0x698098d8)
        MD5_STEP_X2(Ops::F, d1, a1, b1, c1, d2, a2, b2, c2, 9, 12, 0x8b44f7af)
        MD5_STEP_X2(Ops::F, c1, d1, a1, b1, c2, d2, a2, b2, 10, 17, 0xffff5bb1)
        MD5_STEP_X2(Ops::F, b1, c1, d1, a1, b2, c2, d2, a2, 11, 22, 0x895cd7be)
        MD5_STEP_X2(Ops::F, a1, b1, c1, d1, a2, b2, c2, d2, 12, 7, 0x6b901122)
        MD5_STEP_X2(Ops::F, d1, a1, b1, c1, d2, a2, b2, c2, 13, 12, 0xfd987193)
        MD5_STEP_X2(Ops::F, c1, d1, a1, b1, c2, d2, a2, b2, 14, 17, 0xa679438e)
        MD5_STEP_X2(Ops::F, b1, c1, d1, a1, b2, c2, d2, a2, 15, 22, 0x49b40821)

        // Round 2: G, shifts 5/9/14/20
        MD5_STEP_X2(Ops::G, a1, b1, c1, d1, a2, b2, c2, d2, 1, 5, 0xf61e2562)
        MD5_STEP_X2(Ops::G, d1, a1, b1, c1, d2, a2, b2, c2, 6, 9, 0xc040b340)
        MD5_STEP_X2(Ops::G, c1, d1, a1, b1, c2, d2, a2, b2, 11, 14, 0x265e5a51)
        MD5_STEP_X2(Ops::G, b1, c1, d1, a1, b2, c2, d2, a2, 0, 20, 0xe9b6c7aa)
        MD5_STEP_X2(Ops::G, a1, b1, c1, d1, a2, b2, c2, d2, 5, 5, 0xd62f105d)
        MD5_STEP_X2(Ops::G, d1, a1, b1, c1, d2, a2, b2, c2, 10, 9, 0x02441453)
        MD5_STEP_X2(Ops::G, c1, d1, a1, b1, c2, d2, a2, b2, 15, 14, 0xd8a1e681)
        MD5_STEP_X2(Ops::G, b1, c1, d1, a1, b2, c2, d2, a2, 4, 20, 0xe7d3fbc8)
        MD5_STEP_X2(Ops::G, a1, b1, c1, d1, a2, b2, c2, d2, 9, 5, 0x21e1cde6)
        MD5_STEP_X2(Ops::G, d1, a1, b1, c1, d2, a2, b2, c2, 14, 9, 0xc33707d6)
        MD5_STEP_X2(Ops::G, c1, d1, a1, b1, c2, d2, a2, b2, 3, 14, 0xf4d50d87)
        MD5_STEP_X2(Ops::G, b1, c1, d1, a1, b2, c2, d2, a2, 8, 20, 0x455a14ed)
        MD5_STEP_X2(Ops::G, a1, b1, c1, d1, a2, b2, c2, d2, 13, 5, 0xa9e3e905)
        MD5_STEP_X2(Ops::G, d1, a1, b1, c1, d2, a2, b2, c2, 2, 9, 0xfcefa3f8)
        MD5_STEP_X2(Ops::G, c1, d1, a1, b1, c2, d2, a2, b2, 7, 14, 0x676f02d9)
        MD5_STEP_X2(Ops::G, b1, c1, d1, a1, b2, c2, d2, a2, 12, 20, 0x8d2a4c8a)

        // Round 3: H, shifts 4/11/16/23
        MD5_STEP_X2(Ops::H, a1, b1, c1, d1, a2, b2, c2, d2, 5, 4, 0xfffa3942)
        MD5_STEP_X2(Ops::H, d1, a1, b1, c1, d2, a2, b2, c2, 8, 11, 0x8771f681)
        MD5_STEP_X2(Ops::H, c1, d1, a1, b1, c2, d2, a2, b2, 11, 16, 0x6d9d6122)
        MD5_STEP_X2(Ops::H, b1, c1, d1, a1, b2, c2, d2, a2, 14, 23, 0xfde5380c)
        MD5_STEP_X2(Ops::H, a1, b1, c1, d1, a2, b2, c2, d2, 1, 4, 0xa4beea44)
        MD5_STEP_X2(Ops::H, d1, a1, b1, c1, d2, a2, b2, c2, 4, 11, 0x4bdecfa9)
        MD5_STEP_X2(Ops::H, c1, d1, a1, b1, c2, d2, a2, b2, 7, 16, 0xf6bb4b60)
        MD5_STEP_X2(Ops::H, b1, c1, d1, a1, b2, c2, d2, a2, 10, 23, 0xbebfbc70)
        MD5_STEP_X2(Ops::H, a1, b1, c1, d1, a2, b2, c2, d2, 13, 4, 0x289b7ec6)
        MD5_STEP_X2(Ops::H, d1, a1, b1, c1, d2, a2, b2, c2, 0, 11, 0xeaa127fa)
        MD5_STEP_X2(Ops::H, c1, d1, a1, b1, c2, d2, a2, b2, 3, 16, 0xd4ef3085)
        MD5_STEP_X2(Ops::H, b1, c1, d1, a1, b2, c2, d2, a2, 6, 23, 0x04881d05)
        MD5_STEP_X2(Ops::H, a1, b1, c1, d1, a2, b2, c2, d2, 9, 4, 0xd9d4d039)
        MD5_STEP_X2(Ops::H, d1, a1, b1, c1, d2, a2, b2, c2, 12, 11, 0xe6db99e5)
        MD5_STEP_X2(Ops::H, c1, d1, a1, b1, c2, d2, a2, b2, 15, 16, 0x1fa27cf8)
        MD5_STEP_X2(Ops::H, b1, c1, d1, a1, b2, c2, d2, a2, 2, 23, 0xc4ac5665)

        // Round 4: I, shifts 6/10/15/21
        MD5_STEP_X2(Ops::I, a1, b1, c1, d1, a2, b2, c2, d2, 0, 6, 0xf4292244)
        MD5_STEP_X2(Ops::I, d1, a1, b1, c1, d2, a2, b2, c2, 7, 10, 0x432aff97)
        MD5_STEP_X2(Ops::I, c1, d1, a1, b1, c2, d2, a2, b2, 14, 15, 0xab9423a7)
        MD5_STEP_X2(Ops::I, b1, c1, d1, a1, b2, c2, d2, a2, 5, 21, 0xfc93a039)
        MD5_STEP_X2(Ops::I, a1, b1, c1, d1, a2, b2, c2, d2, 12, 6, 0x655b59c3)
        MD5_STEP_X2(Ops::I, d1, a1, b1, c1, d2, a2, b2, c2, 3, 10, 0x8f0ccc92)
        MD5_STEP_X2(Ops::I, c1, d1, a1, b1, c2, d2, a2, b2, 10, 15, 0xffeff47d)
        MD5_STEP_X2(Ops::I, b1, c1, d1, a1, b2, c2, d2, a2, 1, 21, 0x85845dd1)
        MD5_STEP_X2(Ops::I, a1, b1, c1, d1, a2, b2, c2, d2, 8, 6, 0x6fa87e4f)
        MD5_STEP_X2(Ops::I, d1, a1, b1, c1, d2, a2, b2, c2, 15, 10, 0xfe2ce6e0)
        MD5_STEP_X2(Ops::I, c1, d1, a1, b1, c2, d2, a2, b2, 6, 15, 0xa3014314)
        MD5_STEP_X2(Ops::I, b1, c1, d1, a1, b2, c2, d2, a2, 13, 21, 0x4e0811a1)
        MD5_STEP_X2(Ops::I, a1, b1, c1, d1, a2, b2, c2, d2, 4, 6, 0xf7537e82)
        MD5_STEP_X2(Ops::I, d1, a1, b1, c1, d2, a2, b2, c2, 11, 10, 0xbd3af235)
        MD5_STEP_X2(Ops::I, c1, d1, a1, b1, c2, d2, a2, b2, 2, 15, 0x2ad7d2bb)
        MD5_STEP_X2(Ops::I, b1, c1, d1, a1, b2, c2, d2, a2, 9, 21, 0xeb86d391)

        return {
            Ops::add(a1, aa1), Ops::add(b1, bb1), Ops::add(c1, cc1), Ops::add(d1, dd1),
            Ops::add(a2, aa2), Ops::add(b2, bb2), Ops::add(c2, cc2), Ops::add(d2, dd2)};
    }


    /// Extract lane `j` from a SIMD vector as uint32.
    template <typename Ops>
    inline uint32_t extractLane(typename Ops::Vec v, size_t j)
    {
        constexpr size_t N = Ops::lanes;
        alignas(64) uint32_t tmp[N];
        Ops::storeu(tmp, v);
        return tmp[j];
    }


    /// Compute MD5 for up to 2*N lanes, split into two groups of N.
    template <typename Ops>
    void md5MultiBufCompute(const uint8_t * const inputs[], const size_t lengths[], uint8_t * output, size_t actual_count)
    {
        constexpr size_t N = Ops::lanes;
        using Vec = typename Ops::Vec;

        size_t count1 = std::min(actual_count, N);
        size_t count2 = (actual_count > N) ? (actual_count - N) : size_t(0);

        size_t num_blocks[2 * N];
        size_t max_blocks = 0;

        for (size_t j = 0; j < count1; ++j)
        {
            num_blocks[j] = numMD5Blocks(lengths[j]);
            if (num_blocks[j] > max_blocks)
                max_blocks = num_blocks[j];
        }
        for (size_t j = count1; j < N; ++j)
            num_blocks[j] = 1;

        for (size_t j = 0; j < count2; ++j)
        {
            num_blocks[N + j] = numMD5Blocks(lengths[N + j]);
            if (num_blocks[N + j] > max_blocks)
                max_blocks = num_blocks[N + j];
        }
        for (size_t j = count2; j < N; ++j)
            num_blocks[N + j] = 1;

        if (max_blocks == 0)
            max_blocks = 1;

        alignas(64) uint8_t final_buf[2 * N][128];
        size_t final_block_start[2 * N];
        size_t final_block_count[2 * N];

        for (size_t j = 0; j < count1; ++j)
        {
            final_block_start[j] = lengths[j] / 64;
            final_block_count[j] = md5PadFinalBlocks(inputs[j], lengths[j], final_buf[j]);
        }
        for (size_t j = count1; j < N; ++j)
        {
            final_block_start[j] = 0;
            final_block_count[j] = md5PadFinalBlocks(&md5_dummy_lane_byte, 0, final_buf[j]);
        }

        for (size_t j = 0; j < count2; ++j)
        {
            final_block_start[N + j] = lengths[N + j] / 64;
            final_block_count[N + j] = md5PadFinalBlocks(inputs[N + j], lengths[N + j], final_buf[N + j]);
        }
        for (size_t j = count2; j < N; ++j)
        {
            final_block_start[N + j] = 0;
            final_block_count[N + j] = md5PadFinalBlocks(&md5_dummy_lane_byte, 0, final_buf[N + j]);
        }

        Vec a1 = Ops::set1(MD5_A0);
        Vec b1 = Ops::set1(MD5_B0);
        Vec c1 = Ops::set1(MD5_C0);
        Vec d1 = Ops::set1(MD5_D0);
        Vec a2 = Ops::set1(MD5_A0);
        Vec b2 = Ops::set1(MD5_B0);
        Vec c2 = Ops::set1(MD5_C0);
        Vec d2 = Ops::set1(MD5_D0);

        for (size_t blk = 0; blk < max_blocks; ++blk)
        {
            const uint8_t * block_ptrs[2 * N];
            for (size_t idx = 0; idx < 2 * N; ++idx)
            {
                if (blk < final_block_start[idx])
                {
                    block_ptrs[idx] = inputs[idx] + blk * 64;
                }
                else
                {
                    size_t bi = blk - final_block_start[idx];
                    if (bi < final_block_count[idx])
                        block_ptrs[idx] = final_buf[idx] + bi * 64;
                    else
                        block_ptrs[idx] = final_buf[idx];
                }
            }

            Vec msg1[16];
            Vec msg2[16];
            Ops::gatherAllMessageWords(block_ptrs, msg1);
            Ops::gatherAllMessageWords(block_ptrs + N, msg2);

            auto st = md5MultiBufferBlockX2<Ops>(a1, b1, c1, d1, a2, b2, c2, d2, msg1, msg2);
            a1 = st.a1; b1 = st.b1; c1 = st.c1; d1 = st.d1;
            a2 = st.a2; b2 = st.b2; c2 = st.c2; d2 = st.d2;

            for (size_t j = 0; j < count1; ++j)
            {
                if (blk + 1 == num_blocks[j])
                {
                    uint8_t * out = output + j * 16;
                    unalignedStoreLittleEndian<uint32_t>(out,      extractLane<Ops>(a1, j));
                    unalignedStoreLittleEndian<uint32_t>(out + 4,  extractLane<Ops>(b1, j));
                    unalignedStoreLittleEndian<uint32_t>(out + 8,  extractLane<Ops>(c1, j));
                    unalignedStoreLittleEndian<uint32_t>(out + 12, extractLane<Ops>(d1, j));
                }
            }
            for (size_t j = 0; j < count2; ++j)
            {
                if (blk + 1 == num_blocks[N + j])
                {
                    uint8_t * out = output + (N + j) * 16;
                    unalignedStoreLittleEndian<uint32_t>(out,      extractLane<Ops>(a2, j));
                    unalignedStoreLittleEndian<uint32_t>(out + 4,  extractLane<Ops>(b2, j));
                    unalignedStoreLittleEndian<uint32_t>(out + 8,  extractLane<Ops>(c2, j));
                    unalignedStoreLittleEndian<uint32_t>(out + 12, extractLane<Ops>(d2, j));
                }
            }
        }
    }


    /// Batch process ColumnString data using multi-buffer MD5.
    template <typename Ops>
    static void md5BatchColumnString(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnFixedString::Chars & chars_to,
        size_t input_rows_count)
    {
        constexpr size_t N2 = 2 * Ops::lanes;

        ColumnString::Offset current_offset = 0;
        for (size_t base = 0; base < input_rows_count; base += N2)
        {
            size_t batch = std::min(N2, input_rows_count - base);

            const uint8_t * inputs[N2];
            size_t lengths[N2];

            for (size_t j = 0; j < batch; ++j)
            {
                inputs[j] = reinterpret_cast<const uint8_t *>(&data[current_offset]);
                lengths[j] = offsets[base + j] - current_offset;
                current_offset = offsets[base + j];
            }
            for (size_t j = batch; j < N2; ++j)
            {
                inputs[j] = &md5_dummy_lane_byte;
                lengths[j] = 0;
            }

            md5MultiBufCompute<Ops>(inputs, lengths, reinterpret_cast<uint8_t *>(&chars_to[base * MD5_DIGEST_LEN]), batch);
        }
    }

    /// Batch process ColumnFixedString / ColumnIPv6 data (uniform row length).
    template <typename Ops>
    static void md5BatchFixedLen(const uint8_t * data, size_t row_len, ColumnFixedString::Chars & chars_to, size_t input_rows_count)
    {
        constexpr size_t N2 = 2 * Ops::lanes;

        for (size_t base = 0; base < input_rows_count; base += N2)
        {
            size_t batch = std::min(N2, input_rows_count - base);

            const uint8_t * inputs[N2];
            size_t lengths[N2];

            for (size_t j = 0; j < batch; ++j)
            {
                inputs[j] = data + (base + j) * row_len;
                lengths[j] = row_len;
            }
            for (size_t j = batch; j < N2; ++j)
            {
                inputs[j] = &md5_dummy_lane_byte;
                lengths[j] = 0;
            }

            md5MultiBufCompute<Ops>(inputs, lengths, reinterpret_cast<uint8_t *>(&chars_to[base * MD5_DIGEST_LEN]), batch);
        }
    }

    /// Column-type dispatch for multi-buffer MD5.
    template <typename Ops>
    static ColumnPtr executeMD5Batch(const ColumnsWithTypeAndName & arguments, size_t input_rows_count)
    {
        if (const auto * col_from = checkAndGetColumn<ColumnString>(arguments[0].column.get()))
        {
            auto col_to = ColumnFixedString::create(MD5_DIGEST_LEN);
            auto & chars_to = col_to->getChars();
            chars_to.resize(input_rows_count * MD5_DIGEST_LEN);
            md5BatchColumnString<Ops>(col_from->getChars(), col_from->getOffsets(), chars_to, input_rows_count);
            return col_to;
        }

        if (const auto * col_from_fix = checkAndGetColumn<ColumnFixedString>(arguments[0].column.get()))
        {
            auto col_to = ColumnFixedString::create(MD5_DIGEST_LEN);
            auto & chars_to = col_to->getChars();
            chars_to.resize(input_rows_count * MD5_DIGEST_LEN);
            md5BatchFixedLen<Ops>(
                reinterpret_cast<const uint8_t *>(col_from_fix->getChars().data()), col_from_fix->getN(), chars_to, input_rows_count);
            return col_to;
        }

        if (const auto * col_from_ip = checkAndGetColumn<ColumnIPv6>(arguments[0].column.get()))
        {
            auto col_to = ColumnFixedString::create(MD5_DIGEST_LEN);
            auto & chars_to = col_to->getChars();
            chars_to.resize(input_rows_count * MD5_DIGEST_LEN);
            md5BatchFixedLen<Ops>(
                reinterpret_cast<const uint8_t *>(col_from_ip->getData().data()), sizeof(IPv6::UnderlyingType), chars_to, input_rows_count);
            return col_to;
        }

        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function MD5", arguments[0].column->getName());
    }

    ) // DECLARE_MULTITARGET_CODE

#undef MD5_STEP_X2


/// Scalar (2 lanes x 2 groups = 4 digests per iteration).
/// Uses a plain struct of uint32_t so each element stays in its own register.
/// This gives the OOO engine 4 independent dependency chains to fill ALU ports,
/// without the cross-lane carry/shift issues of SWAR packing.
DECLARE_DEFAULT_CODE(

struct ScalarMD5Ops
{
    struct Vec
    {
        uint32_t v[2];
    };
    static constexpr size_t lanes = 2;

    static inline Vec add(Vec a, Vec b)
    {
        return {a.v[0] + b.v[0], a.v[1] + b.v[1]};
    }
    static inline Vec set1(uint32_t val)
    {
        return {val, val};
    }
    static inline Vec loadu(const void * p)
    {
        Vec r;
        std::memcpy(r.v, p, sizeof(r.v));
        return r;
    }
    static inline void storeu(void * p, Vec val)
    {
        std::memcpy(p, val.v, sizeof(val.v));
    }

    template <int N>
    static inline Vec rotl(Vec x)
    {
        return {(x.v[0] << N) | (x.v[0] >> (32 - N)), (x.v[1] << N) | (x.v[1] >> (32 - N))};
    }

    static inline Vec F(Vec b, Vec c, Vec d)
    {
        return {d.v[0] ^ (b.v[0] & (c.v[0] ^ d.v[0])), d.v[1] ^ (b.v[1] & (c.v[1] ^ d.v[1]))};
    }
    static inline Vec G(Vec b, Vec c, Vec d)
    {
        return {c.v[0] ^ (d.v[0] & (b.v[0] ^ c.v[0])), c.v[1] ^ (d.v[1] & (b.v[1] ^ c.v[1]))};
    }
    static inline Vec H(Vec b, Vec c, Vec d)
    {
        return {b.v[0] ^ c.v[0] ^ d.v[0], b.v[1] ^ c.v[1] ^ d.v[1]};
    }
    static inline Vec I(Vec b, Vec c, Vec d)
    {
        return {c.v[0] ^ (b.v[0] | ~d.v[0]), c.v[1] ^ (b.v[1] | ~d.v[1])};
    }

    static inline void gatherAllMessageWords(const uint8_t * const block_ptrs[], Vec msg[16])
    {
        for (int i = 0; i < 16; ++i)
        {
            msg[i].v[0] = unalignedLoadLittleEndian<uint32_t>(block_ptrs[0] + i * 4);
            msg[i].v[1] = unalignedLoadLittleEndian<uint32_t>(block_ptrs[1] + i * 4);
        }
    }
};

class FunctionMD5Impl : public FunctionMD5Base
{
public:
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        return executeMD5Batch<ScalarMD5Ops>(arguments, input_rows_count);
    }
};

) // DECLARE_DEFAULT_CODE


/// AVX2 (8 lanes x 2 groups = 16 parallel digests)
DECLARE_X86_64_V3_SPECIFIC_CODE(

struct AVX2MD5Ops
{
    using Vec = __m256i;
    static constexpr size_t lanes = 8;

    static inline Vec add(Vec a, Vec b)
    {
        return _mm256_add_epi32(a, b);
    }
    static inline Vec set1(uint32_t v)
    {
        return _mm256_set1_epi32(static_cast<int>(v));
    }
    static inline Vec loadu(const void * p)
    {
        return _mm256_loadu_si256(reinterpret_cast<const __m256i *>(p));
    }
    static inline void storeu(void * p, Vec v)
    {
        _mm256_storeu_si256(reinterpret_cast<__m256i *>(p), v);
    }

    template <int N>
    static inline Vec rotl(Vec x)
    {
        return _mm256_or_si256(_mm256_slli_epi32(x, N), _mm256_srli_epi32(x, 32 - N));
    }

    static inline Vec F(Vec b, Vec c, Vec d)
    {
        return _mm256_xor_si256(d, _mm256_and_si256(b, _mm256_xor_si256(c, d)));
    }
    static inline Vec G(Vec b, Vec c, Vec d)
    {
        return _mm256_xor_si256(c, _mm256_and_si256(d, _mm256_xor_si256(b, c)));
    }
    static inline Vec H(Vec b, Vec c, Vec d)
    {
        return _mm256_xor_si256(b, _mm256_xor_si256(c, d));
    }
    static inline Vec I(Vec b, Vec c, Vec d)
    {
        return _mm256_xor_si256(c, _mm256_or_si256(b, _mm256_xor_si256(d, _mm256_set1_epi32(-1))));
    }

    /// 8x8 transpose in two halves (words 0-7 then 8-15).
    static inline void gatherAllMessageWords(const uint8_t * const block_ptrs[], Vec msg[16])
    {
        for (int half = 0; half < 2; ++half)
        {
            size_t off = half * 32;
            Vec r0 = loadu(block_ptrs[0] + off);
            Vec r1 = loadu(block_ptrs[1] + off);
            Vec r2 = loadu(block_ptrs[2] + off);
            Vec r3 = loadu(block_ptrs[3] + off);
            Vec r4 = loadu(block_ptrs[4] + off);
            Vec r5 = loadu(block_ptrs[5] + off);
            Vec r6 = loadu(block_ptrs[6] + off);
            Vec r7 = loadu(block_ptrs[7] + off);

            Vec t0 = _mm256_unpacklo_epi32(r0, r1);
            Vec t1 = _mm256_unpackhi_epi32(r0, r1);
            Vec t2 = _mm256_unpacklo_epi32(r2, r3);
            Vec t3 = _mm256_unpackhi_epi32(r2, r3);
            Vec t4 = _mm256_unpacklo_epi32(r4, r5);
            Vec t5 = _mm256_unpackhi_epi32(r4, r5);
            Vec t6 = _mm256_unpacklo_epi32(r6, r7);
            Vec t7 = _mm256_unpackhi_epi32(r6, r7);

            Vec u0 = _mm256_unpacklo_epi64(t0, t2);
            Vec u1 = _mm256_unpackhi_epi64(t0, t2);
            Vec u2 = _mm256_unpacklo_epi64(t1, t3);
            Vec u3 = _mm256_unpackhi_epi64(t1, t3);
            Vec u4 = _mm256_unpacklo_epi64(t4, t6);
            Vec u5 = _mm256_unpackhi_epi64(t4, t6);
            Vec u6 = _mm256_unpacklo_epi64(t5, t7);
            Vec u7 = _mm256_unpackhi_epi64(t5, t7);

            size_t base = half * 8;
            msg[base + 0] = _mm256_permute2x128_si256(u0, u4, 0x20);
            msg[base + 4] = _mm256_permute2x128_si256(u0, u4, 0x31);
            msg[base + 1] = _mm256_permute2x128_si256(u1, u5, 0x20);
            msg[base + 5] = _mm256_permute2x128_si256(u1, u5, 0x31);
            msg[base + 2] = _mm256_permute2x128_si256(u2, u6, 0x20);
            msg[base + 6] = _mm256_permute2x128_si256(u2, u6, 0x31);
            msg[base + 3] = _mm256_permute2x128_si256(u3, u7, 0x20);
            msg[base + 7] = _mm256_permute2x128_si256(u3, u7, 0x31);
        }
    }
};

class FunctionMD5Impl : public FunctionMD5Base
{
public:
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        return executeMD5Batch<AVX2MD5Ops>(arguments, input_rows_count);
    }
};

) // DECLARE_X86_64_V3_SPECIFIC_CODE


/// AVX-512 (16 lanes x 2 groups = 32 parallel digests)
DECLARE_X86_64_V4_SPECIFIC_CODE(

struct AVX512MD5Ops
{
    using Vec = __m512i;
    static constexpr size_t lanes = 16;

    static inline Vec add(Vec a, Vec b)
    {
        return _mm512_add_epi32(a, b);
    }
    static inline Vec set1(uint32_t v)
    {
        return _mm512_set1_epi32(static_cast<int>(v));
    }
    static inline Vec loadu(const void * p)
    {
        return _mm512_loadu_si512(p);
    }
    static inline void storeu(void * p, Vec v)
    {
        _mm512_storeu_si512(p, v);
    }

    template <int N>
    static inline Vec rotl(Vec x)
    {
        return _mm512_rol_epi32(x, N);
    }

    /// Ternary logic: single-instruction 3-input boolean functions.
    static inline Vec F(Vec b, Vec c, Vec d)
    {
        return _mm512_ternarylogic_epi32(b, c, d, 0xCA);
    }
    static inline Vec G(Vec b, Vec c, Vec d)
    {
        return _mm512_ternarylogic_epi32(b, c, d, 0xE4);
    }
    static inline Vec H(Vec b, Vec c, Vec d)
    {
        return _mm512_ternarylogic_epi32(b, c, d, 0x96);
    }
    static inline Vec I(Vec b, Vec c, Vec d)
    {
        return _mm512_ternarylogic_epi32(b, c, d, 0x39);
    }

    /// 16x16 transpose: each lane's block is 64 bytes = one __m512i load.
    /// Similar to (but no vshufps, vpermi2q, vshuff64x2): intel/isa-l_crypto md5_mb/md5_mb_x16x2_avx512.asm TRANSPOSE16 macro.
    static inline void gatherAllMessageWords(const uint8_t * const block_ptrs[], Vec msg[16])
    {
        Vec r0 = loadu(block_ptrs[0]);
        Vec r1 = loadu(block_ptrs[1]);
        Vec r2 = loadu(block_ptrs[2]);
        Vec r3 = loadu(block_ptrs[3]);
        Vec r4 = loadu(block_ptrs[4]);
        Vec r5 = loadu(block_ptrs[5]);
        Vec r6 = loadu(block_ptrs[6]);
        Vec r7 = loadu(block_ptrs[7]);
        Vec r8 = loadu(block_ptrs[8]);
        Vec r9 = loadu(block_ptrs[9]);
        Vec r10 = loadu(block_ptrs[10]);
        Vec r11 = loadu(block_ptrs[11]);
        Vec r12 = loadu(block_ptrs[12]);
        Vec r13 = loadu(block_ptrs[13]);
        Vec r14 = loadu(block_ptrs[14]);
        Vec r15 = loadu(block_ptrs[15]);

        Vec t0 = _mm512_unpacklo_epi32(r0, r1);
        Vec t1 = _mm512_unpackhi_epi32(r0, r1);
        Vec t2 = _mm512_unpacklo_epi32(r2, r3);
        Vec t3 = _mm512_unpackhi_epi32(r2, r3);
        Vec t4 = _mm512_unpacklo_epi32(r4, r5);
        Vec t5 = _mm512_unpackhi_epi32(r4, r5);
        Vec t6 = _mm512_unpacklo_epi32(r6, r7);
        Vec t7 = _mm512_unpackhi_epi32(r6, r7);
        Vec t8 = _mm512_unpacklo_epi32(r8, r9);
        Vec t9 = _mm512_unpackhi_epi32(r8, r9);
        Vec t10 = _mm512_unpacklo_epi32(r10, r11);
        Vec t11 = _mm512_unpackhi_epi32(r10, r11);
        Vec t12 = _mm512_unpacklo_epi32(r12, r13);
        Vec t13 = _mm512_unpackhi_epi32(r12, r13);
        Vec t14 = _mm512_unpacklo_epi32(r14, r15);
        Vec t15 = _mm512_unpackhi_epi32(r14, r15);

        Vec u0 = _mm512_unpacklo_epi64(t0, t2);
        Vec u1 = _mm512_unpackhi_epi64(t0, t2);
        Vec u2 = _mm512_unpacklo_epi64(t1, t3);
        Vec u3 = _mm512_unpackhi_epi64(t1, t3);
        Vec u4 = _mm512_unpacklo_epi64(t4, t6);
        Vec u5 = _mm512_unpackhi_epi64(t4, t6);
        Vec u6 = _mm512_unpacklo_epi64(t5, t7);
        Vec u7 = _mm512_unpackhi_epi64(t5, t7);
        Vec u8 = _mm512_unpacklo_epi64(t8, t10);
        Vec u9 = _mm512_unpackhi_epi64(t8, t10);
        Vec u10 = _mm512_unpacklo_epi64(t9, t11);
        Vec u11 = _mm512_unpackhi_epi64(t9, t11);
        Vec u12 = _mm512_unpacklo_epi64(t12, t14);
        Vec u13 = _mm512_unpackhi_epi64(t12, t14);
        Vec u14 = _mm512_unpacklo_epi64(t13, t15);
        Vec u15 = _mm512_unpackhi_epi64(t13, t15);

        Vec v0 = _mm512_shuffle_i32x4(u0, u4, 0x44);
        Vec v1 = _mm512_shuffle_i32x4(u0, u4, 0xEE);
        Vec v2 = _mm512_shuffle_i32x4(u1, u5, 0x44);
        Vec v3 = _mm512_shuffle_i32x4(u1, u5, 0xEE);
        Vec v4 = _mm512_shuffle_i32x4(u2, u6, 0x44);
        Vec v5 = _mm512_shuffle_i32x4(u2, u6, 0xEE);
        Vec v6 = _mm512_shuffle_i32x4(u3, u7, 0x44);
        Vec v7 = _mm512_shuffle_i32x4(u3, u7, 0xEE);
        Vec v8 = _mm512_shuffle_i32x4(u8, u12, 0x44);
        Vec v9 = _mm512_shuffle_i32x4(u8, u12, 0xEE);
        Vec v10 = _mm512_shuffle_i32x4(u9, u13, 0x44);
        Vec v11 = _mm512_shuffle_i32x4(u9, u13, 0xEE);
        Vec v12 = _mm512_shuffle_i32x4(u10, u14, 0x44);
        Vec v13 = _mm512_shuffle_i32x4(u10, u14, 0xEE);
        Vec v14 = _mm512_shuffle_i32x4(u11, u15, 0x44);
        Vec v15 = _mm512_shuffle_i32x4(u11, u15, 0xEE);

        msg[0] = _mm512_shuffle_i32x4(v0, v8, 0x88);
        msg[4] = _mm512_shuffle_i32x4(v0, v8, 0xDD);
        msg[1] = _mm512_shuffle_i32x4(v2, v10, 0x88);
        msg[5] = _mm512_shuffle_i32x4(v2, v10, 0xDD);
        msg[2] = _mm512_shuffle_i32x4(v4, v12, 0x88);
        msg[6] = _mm512_shuffle_i32x4(v4, v12, 0xDD);
        msg[3] = _mm512_shuffle_i32x4(v6, v14, 0x88);
        msg[7] = _mm512_shuffle_i32x4(v6, v14, 0xDD);
        msg[8] = _mm512_shuffle_i32x4(v1, v9, 0x88);
        msg[12] = _mm512_shuffle_i32x4(v1, v9, 0xDD);
        msg[9] = _mm512_shuffle_i32x4(v3, v11, 0x88);
        msg[13] = _mm512_shuffle_i32x4(v3, v11, 0xDD);
        msg[10] = _mm512_shuffle_i32x4(v5, v13, 0x88);
        msg[14] = _mm512_shuffle_i32x4(v5, v13, 0xDD);
        msg[11] = _mm512_shuffle_i32x4(v7, v15, 0x88);
        msg[15] = _mm512_shuffle_i32x4(v7, v15, 0xDD);
    }
};

class FunctionMD5Impl : public FunctionMD5Base
{
public:
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        return executeMD5Batch<AVX512MD5Ops>(arguments, input_rows_count);
    }
};

) // DECLARE_X86_64_V4_SPECIFIC_CODE


#ifndef MD5_GTEST_UNIT_TEST

/// Runtime dispatch via ImplementationSelector.
class FunctionMD5 : public TargetSpecific::Default::FunctionMD5Impl
{
public:
    explicit FunctionMD5(ContextPtr context)
        : selector(context)
    {
#if USE_SSL
        if (OpenSSLInitializer::instance().isFIPSEnabled())
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Function {} is not available in FIPS mode", name);
#endif

        selector.registerImplementation<TargetArch::Default, TargetSpecific::Default::FunctionMD5Impl>();

#if USE_MULTITARGET_CODE
        selector.registerImplementation<TargetArch::x86_64_v3, TargetSpecific::x86_64_v3::FunctionMD5Impl>();
        selector.registerImplementation<TargetArch::x86_64_v4, TargetSpecific::x86_64_v4::FunctionMD5Impl>();
#endif
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return selector.selectAndExecute(arguments, result_type, input_rows_count);
    }

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionMD5>(context); }

private:
    ImplementationSelector<IFunction> selector;
};


REGISTER_FUNCTION(MD5)
{
    FunctionDocumentation::Description description_MD5 = R"(
Calculates the MD5 hash of the given string.
    )";
    FunctionDocumentation::Syntax syntax_MD5 = "MD5(s)";
    FunctionDocumentation::Arguments arguments_MD5 = {{"s", "The input string to hash.", {"String"}}};
    FunctionDocumentation::ReturnedValue returned_value_MD5
        = {"Returns the MD5 hash of the given input string as a fixed-length string.", {"FixedString(16)"}};
    FunctionDocumentation::Examples example_MD5
        = {{"Usage example",
            R"(
SELECT HEX(MD5('abc'));
        )",
            R"(
┌─hex(MD5('abc'))──────────────────┐
│ 900150983CD24FB0D6963F7D28E17F72 │
└──────────────────────────────────┘
        )"}};
    FunctionDocumentation::IntroducedIn introduced_in_MD5 = {1, 1};
    FunctionDocumentation::Category category_MD5 = FunctionDocumentation::Category::Hash;
    FunctionDocumentation documentation_MD5
        = {description_MD5, syntax_MD5, arguments_MD5, {}, returned_value_MD5, example_MD5, introduced_in_MD5, category_MD5};

    factory.registerFunction<FunctionMD5>(documentation_MD5);
}

#endif // MD5_GTEST_UNIT_TEST

}
