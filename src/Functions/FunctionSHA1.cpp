/// Multi-buffer SHA-1 function.
///
/// Processes multiple independent SHA-1 digests in parallel using SIMD.
/// On x86-64 with AVX-512, computes 16 digests simultaneously.
/// On other architectures, falls back to OpenSSL (which uses hardware
/// SHA-1 instructions: SHA-NI on x86, ARM CE on ARM).

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/PerformanceAdaptors.h>
#include <base/IPv4andIPv6.h>
#include <base/defines.h>
#include <base/unaligned.h>

#include <Common/TargetSpecific.h>

#include "config.h"

#if USE_SSL
#    include <Common/Crypto/OpenSSLInitializer.h>
#    include <Common/OpenSSLHelpers.h>
#    include <openssl/evp.h>
#endif

#if USE_MULTITARGET_CODE && (defined(__x86_64__) || defined(_M_X64))
#    include <immintrin.h>
#endif

#include <algorithm>
#include <cstdint>
#include <cstring>

namespace
{

/// SHA-1 initial state (FIPS 180-4).
constexpr uint32_t SHA1_H0 = 0x67452301;
constexpr uint32_t SHA1_H1 = 0xEFCDAB89;
constexpr uint32_t SHA1_H2 = 0x98BADCFE;
constexpr uint32_t SHA1_H3 = 0x10325476;
constexpr uint32_t SHA1_H4 = 0xC3D2E1F0;

/// SHA-1 round constants.
constexpr uint32_t SHA1_K0 = 0x5A827999;
constexpr uint32_t SHA1_K1 = 0x6ED9EBA1;
constexpr uint32_t SHA1_K2 = 0x8F1BBCDC;
constexpr uint32_t SHA1_K3 = 0xCA62C1D6;

constexpr size_t SHA1_DIGEST_LEN = 20;

/// Placeholder for unused SIMD lanes. Only passed with length=0, so zero bytes are read from it.
constexpr uint8_t sha1_dummy_lane_byte = 0;

/// Pad a message per SHA-1 spec. Writes the final 1-2 blocks into `out`.
/// Returns the number of final blocks written (1 or 2).
[[maybe_unused]] size_t sha1PadFinalBlocks(const uint8_t * data, size_t len, uint8_t * out)
{
    size_t full_blocks = len / 64;
    size_t tail = len % 64;
    size_t num_blocks = (len + 9 + 63) / 64;
    size_t final_count = num_blocks - full_blocks;

    std::memset(out, 0, final_count * 64);
    std::memcpy(out, data + full_blocks * 64, tail);
    out[tail] = 0x80;

    uint64_t bit_len = static_cast<uint64_t>(len) * 8;
    unalignedStoreBigEndian<uint64_t>(out + final_count * 64 - 8, bit_len);

    return final_count;
}

size_t numSHA1Blocks(size_t len)
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
extern const int OPENSSL_ERROR;
extern const int SUPPORT_IS_DISABLED;
}

/// Shared base class: common IFunction overrides.
class FunctionSHA1Base : public IFunction
{
public:
    static constexpr auto name = "SHA1";

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    bool useDefaultImplementationForVariant() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isStringOrFixedString(arguments[0]) && !isIPv6(arguments[0])) [[unlikely]]
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", arguments[0]->getName(), getName());
        return std::make_shared<DataTypeFixedString>(SHA1_DIGEST_LEN);
    }
};


/// ============================================================
/// Multi-buffer SHA-1, templated on Ops.
///
/// Template functions go inside DECLARE_MULTITARGET_CODE so each
/// target-specific copy is compiled with the correct ISA flags.
/// The Ops struct (AVX512SHA1Ops) is in its own DECLARE block
/// and found via same-namespace lookup.
/// ============================================================

/// One SHA-1 round step for a single group of N lanes.
/// Defined outside DECLARE_MULTITARGET_CODE because preprocessor
/// directives inside macro arguments are undefined behavior.
#define SHA1_STEP(func, a, b, c, d, e, wi, k) \
    { \
        Vec t = func(b, c, d); \
        t = Ops::add(t, e); \
        t = Ops::add(t, Ops::template rotl<5>(a)); \
        t = Ops::add(t, Ops::set1(k)); \
        t = Ops::add(t, wi); \
        (e) = t; \
        (b) = Ops::template rotl<30>(b); \
    }

/// Message schedule expansion for rounds >= 16.
/// Uses 3-way XOR (vpternlogd on AVX-512) to save one instruction vs chained XORs.
#define SHA1_EXPAND(w, i) \
    { \
        (w)[(i) & 15] = Ops::template rotl<1>( \
            Ops::xor_(Ops::xor3((w)[(i) & 15], (w)[((i) - 3) & 15], (w)[((i) - 8) & 15]), (w)[((i) - 14) & 15])); \
    }

/// Combined expand + step for rounds >= 16.
#define SHA1_ROUND(func, a, b, c, d, e, i, k) \
    SHA1_EXPAND(w, i) \
    SHA1_STEP(func, a, b, c, d, e, w[(i) & 15], k)

DECLARE_MULTITARGET_CODE(

    template <typename Ops> struct SHA1State { typename Ops::Vec a, b, c, d, e; };

    /// Process one 64-byte block for a single group of N lanes.
    template <typename Ops>
    ALWAYS_INLINE SHA1State<Ops> sha1MultiBufferBlock(
        typename Ops::Vec a,
        typename Ops::Vec b,
        typename Ops::Vec c,
        typename Ops::Vec d,
        typename Ops::Vec e,
        typename Ops::Vec w[16])
    {
        using Vec = typename Ops::Vec;
        Vec aa = a;
        Vec bb = b;
        Vec cc = c;
        Vec dd = d;
        Vec ee = e;

        /// Rounds 0-15: use message words directly (already in w).
        /// Register rotation pattern for 5-word state: (a,b,c,d,e) -> (e,a,b,c,d) -> ...
        SHA1_STEP(Ops::F1, a, b, c, d, e, w[0], SHA1_K0)
        SHA1_STEP(Ops::F1, e, a, b, c, d, w[1], SHA1_K0)
        SHA1_STEP(Ops::F1, d, e, a, b, c, w[2], SHA1_K0)
        SHA1_STEP(Ops::F1, c, d, e, a, b, w[3], SHA1_K0)
        SHA1_STEP(Ops::F1, b, c, d, e, a, w[4], SHA1_K0)
        SHA1_STEP(Ops::F1, a, b, c, d, e, w[5], SHA1_K0)
        SHA1_STEP(Ops::F1, e, a, b, c, d, w[6], SHA1_K0)
        SHA1_STEP(Ops::F1, d, e, a, b, c, w[7], SHA1_K0)
        SHA1_STEP(Ops::F1, c, d, e, a, b, w[8], SHA1_K0)
        SHA1_STEP(Ops::F1, b, c, d, e, a, w[9], SHA1_K0)
        SHA1_STEP(Ops::F1, a, b, c, d, e, w[10], SHA1_K0)
        SHA1_STEP(Ops::F1, e, a, b, c, d, w[11], SHA1_K0)
        SHA1_STEP(Ops::F1, d, e, a, b, c, w[12], SHA1_K0)
        SHA1_STEP(Ops::F1, c, d, e, a, b, w[13], SHA1_K0)
        SHA1_STEP(Ops::F1, b, c, d, e, a, w[14], SHA1_K0)
        SHA1_STEP(Ops::F1, a, b, c, d, e, w[15], SHA1_K0)

        /// Rounds 16-19: expand + F1
        SHA1_ROUND(Ops::F1, e, a, b, c, d, 16, SHA1_K0)
        SHA1_ROUND(Ops::F1, d, e, a, b, c, 17, SHA1_K0)
        SHA1_ROUND(Ops::F1, c, d, e, a, b, 18, SHA1_K0)
        SHA1_ROUND(Ops::F1, b, c, d, e, a, 19, SHA1_K0)

        /// Rounds 20-39: expand + F2
        SHA1_ROUND(Ops::F2, a, b, c, d, e, 20, SHA1_K1)
        SHA1_ROUND(Ops::F2, e, a, b, c, d, 21, SHA1_K1)
        SHA1_ROUND(Ops::F2, d, e, a, b, c, 22, SHA1_K1)
        SHA1_ROUND(Ops::F2, c, d, e, a, b, 23, SHA1_K1)
        SHA1_ROUND(Ops::F2, b, c, d, e, a, 24, SHA1_K1)
        SHA1_ROUND(Ops::F2, a, b, c, d, e, 25, SHA1_K1)
        SHA1_ROUND(Ops::F2, e, a, b, c, d, 26, SHA1_K1)
        SHA1_ROUND(Ops::F2, d, e, a, b, c, 27, SHA1_K1)
        SHA1_ROUND(Ops::F2, c, d, e, a, b, 28, SHA1_K1)
        SHA1_ROUND(Ops::F2, b, c, d, e, a, 29, SHA1_K1)
        SHA1_ROUND(Ops::F2, a, b, c, d, e, 30, SHA1_K1)
        SHA1_ROUND(Ops::F2, e, a, b, c, d, 31, SHA1_K1)
        SHA1_ROUND(Ops::F2, d, e, a, b, c, 32, SHA1_K1)
        SHA1_ROUND(Ops::F2, c, d, e, a, b, 33, SHA1_K1)
        SHA1_ROUND(Ops::F2, b, c, d, e, a, 34, SHA1_K1)
        SHA1_ROUND(Ops::F2, a, b, c, d, e, 35, SHA1_K1)
        SHA1_ROUND(Ops::F2, e, a, b, c, d, 36, SHA1_K1)
        SHA1_ROUND(Ops::F2, d, e, a, b, c, 37, SHA1_K1)
        SHA1_ROUND(Ops::F2, c, d, e, a, b, 38, SHA1_K1)
        SHA1_ROUND(Ops::F2, b, c, d, e, a, 39, SHA1_K1)

        /// Rounds 40-59: expand + F3
        SHA1_ROUND(Ops::F3, a, b, c, d, e, 40, SHA1_K2)
        SHA1_ROUND(Ops::F3, e, a, b, c, d, 41, SHA1_K2)
        SHA1_ROUND(Ops::F3, d, e, a, b, c, 42, SHA1_K2)
        SHA1_ROUND(Ops::F3, c, d, e, a, b, 43, SHA1_K2)
        SHA1_ROUND(Ops::F3, b, c, d, e, a, 44, SHA1_K2)
        SHA1_ROUND(Ops::F3, a, b, c, d, e, 45, SHA1_K2)
        SHA1_ROUND(Ops::F3, e, a, b, c, d, 46, SHA1_K2)
        SHA1_ROUND(Ops::F3, d, e, a, b, c, 47, SHA1_K2)
        SHA1_ROUND(Ops::F3, c, d, e, a, b, 48, SHA1_K2)
        SHA1_ROUND(Ops::F3, b, c, d, e, a, 49, SHA1_K2)
        SHA1_ROUND(Ops::F3, a, b, c, d, e, 50, SHA1_K2)
        SHA1_ROUND(Ops::F3, e, a, b, c, d, 51, SHA1_K2)
        SHA1_ROUND(Ops::F3, d, e, a, b, c, 52, SHA1_K2)
        SHA1_ROUND(Ops::F3, c, d, e, a, b, 53, SHA1_K2)
        SHA1_ROUND(Ops::F3, b, c, d, e, a, 54, SHA1_K2)
        SHA1_ROUND(Ops::F3, a, b, c, d, e, 55, SHA1_K2)
        SHA1_ROUND(Ops::F3, e, a, b, c, d, 56, SHA1_K2)
        SHA1_ROUND(Ops::F3, d, e, a, b, c, 57, SHA1_K2)
        SHA1_ROUND(Ops::F3, c, d, e, a, b, 58, SHA1_K2)
        SHA1_ROUND(Ops::F3, b, c, d, e, a, 59, SHA1_K2)

        /// Rounds 60-79: expand + F4
        SHA1_ROUND(Ops::F4, a, b, c, d, e, 60, SHA1_K3)
        SHA1_ROUND(Ops::F4, e, a, b, c, d, 61, SHA1_K3)
        SHA1_ROUND(Ops::F4, d, e, a, b, c, 62, SHA1_K3)
        SHA1_ROUND(Ops::F4, c, d, e, a, b, 63, SHA1_K3)
        SHA1_ROUND(Ops::F4, b, c, d, e, a, 64, SHA1_K3)
        SHA1_ROUND(Ops::F4, a, b, c, d, e, 65, SHA1_K3)
        SHA1_ROUND(Ops::F4, e, a, b, c, d, 66, SHA1_K3)
        SHA1_ROUND(Ops::F4, d, e, a, b, c, 67, SHA1_K3)
        SHA1_ROUND(Ops::F4, c, d, e, a, b, 68, SHA1_K3)
        SHA1_ROUND(Ops::F4, b, c, d, e, a, 69, SHA1_K3)
        SHA1_ROUND(Ops::F4, a, b, c, d, e, 70, SHA1_K3)
        SHA1_ROUND(Ops::F4, e, a, b, c, d, 71, SHA1_K3)
        SHA1_ROUND(Ops::F4, d, e, a, b, c, 72, SHA1_K3)
        SHA1_ROUND(Ops::F4, c, d, e, a, b, 73, SHA1_K3)
        SHA1_ROUND(Ops::F4, b, c, d, e, a, 74, SHA1_K3)
        SHA1_ROUND(Ops::F4, a, b, c, d, e, 75, SHA1_K3)
        SHA1_ROUND(Ops::F4, e, a, b, c, d, 76, SHA1_K3)
        SHA1_ROUND(Ops::F4, d, e, a, b, c, 77, SHA1_K3)
        SHA1_ROUND(Ops::F4, c, d, e, a, b, 78, SHA1_K3)
        SHA1_ROUND(Ops::F4, b, c, d, e, a, 79, SHA1_K3)

        return {Ops::add(a, aa), Ops::add(b, bb), Ops::add(c, cc), Ops::add(d, dd), Ops::add(e, ee)};
    }


    /// Extract lane `j` from a SIMD vector as uint32.
    template <typename Ops>
    inline uint32_t extractLane(typename Ops::Vec v, size_t j)
    {
        return Ops::extract(v, j);
    }


    /// Compute SHA-1 for up to N lanes in a single group.
    template <typename Ops>
    void sha1MultiBufCompute(const uint8_t * const inputs[], const size_t lengths[], uint8_t * output, size_t actual_count)
    {
        constexpr size_t N = Ops::lanes;
        using Vec = typename Ops::Vec;

        size_t num_blocks[N];
        size_t max_blocks = 0;

        for (size_t j = 0; j < actual_count; ++j)
        {
            num_blocks[j] = numSHA1Blocks(lengths[j]);
            if (num_blocks[j] > max_blocks)
                max_blocks = num_blocks[j];
        }
        for (size_t j = actual_count; j < N; ++j)
            num_blocks[j] = 1;

        if (max_blocks == 0)
            max_blocks = 1;

        alignas(64) uint8_t final_buf[N][128];
        size_t final_block_start[N];
        size_t final_block_count[N];

        for (size_t j = 0; j < actual_count; ++j)
        {
            final_block_start[j] = lengths[j] / 64;
            final_block_count[j] = sha1PadFinalBlocks(inputs[j], lengths[j], final_buf[j]);
        }
        for (size_t j = actual_count; j < N; ++j)
        {
            final_block_start[j] = 0;
            final_block_count[j] = sha1PadFinalBlocks(&sha1_dummy_lane_byte, 0, final_buf[j]);
        }

        Vec a = Ops::set1(SHA1_H0);
        Vec b = Ops::set1(SHA1_H1);
        Vec c = Ops::set1(SHA1_H2);
        Vec d = Ops::set1(SHA1_H3);
        Vec e = Ops::set1(SHA1_H4);

        for (size_t blk = 0; blk < max_blocks; ++blk)
        {
            const uint8_t * block_ptrs[N];
            for (size_t idx = 0; idx < N; ++idx)
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

            Vec msg[16];
            Ops::gatherAllMessageWords(block_ptrs, msg);

            auto st = sha1MultiBufferBlock<Ops>(a, b, c, d, e, msg);
            a = st.a;
            b = st.b;
            c = st.c;
            d = st.d;
            e = st.e;

            for (size_t j = 0; j < actual_count; ++j)
            {
                if (blk + 1 == num_blocks[j])
                {
                    uint8_t * out = output + j * 20;
                    unalignedStoreBigEndian<uint32_t>(out, extractLane<Ops>(a, j));
                    unalignedStoreBigEndian<uint32_t>(out + 4, extractLane<Ops>(b, j));
                    unalignedStoreBigEndian<uint32_t>(out + 8, extractLane<Ops>(c, j));
                    unalignedStoreBigEndian<uint32_t>(out + 12, extractLane<Ops>(d, j));
                    unalignedStoreBigEndian<uint32_t>(out + 16, extractLane<Ops>(e, j));
                }
            }
        }
    }


    /// Batch process ColumnString data using multi-buffer SHA-1.
    template <typename Ops>
    static void sha1BatchColumnString(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnFixedString::Chars & chars_to,
        size_t input_rows_count)
    {
        constexpr size_t N = Ops::lanes;

        ColumnString::Offset current_offset = 0;
        for (size_t base = 0; base < input_rows_count; base += N)
        {
            size_t batch = std::min(N, input_rows_count - base);

            const uint8_t * inputs[N];
            size_t lengths[N];

            for (size_t j = 0; j < batch; ++j)
            {
                inputs[j] = reinterpret_cast<const uint8_t *>(&data[current_offset]);
                lengths[j] = offsets[base + j] - current_offset;
                current_offset = offsets[base + j];
            }
            for (size_t j = batch; j < N; ++j)
            {
                inputs[j] = &sha1_dummy_lane_byte;
                lengths[j] = 0;
            }

            sha1MultiBufCompute<Ops>(inputs, lengths, reinterpret_cast<uint8_t *>(&chars_to[base * SHA1_DIGEST_LEN]), batch);
        }
    }

    /// Batch process ColumnFixedString / ColumnIPv6 data (uniform row length).
    template <typename Ops>
    static void sha1BatchFixedLen(const uint8_t * data, size_t row_len, ColumnFixedString::Chars & chars_to, size_t input_rows_count)
    {
        constexpr size_t N = Ops::lanes;

        for (size_t base = 0; base < input_rows_count; base += N)
        {
            size_t batch = std::min(N, input_rows_count - base);

            const uint8_t * inputs[N];
            size_t lengths[N];

            for (size_t j = 0; j < batch; ++j)
            {
                inputs[j] = data + (base + j) * row_len;
                lengths[j] = row_len;
            }
            for (size_t j = batch; j < N; ++j)
            {
                inputs[j] = &sha1_dummy_lane_byte;
                lengths[j] = 0;
            }

            sha1MultiBufCompute<Ops>(inputs, lengths, reinterpret_cast<uint8_t *>(&chars_to[base * SHA1_DIGEST_LEN]), batch);
        }
    }

    /// Column-type dispatch for multi-buffer SHA-1.
    template <typename Ops>
    static ColumnPtr executeSHA1Batch(const ColumnsWithTypeAndName & arguments, size_t input_rows_count)
    {
        if (const auto * col_from = checkAndGetColumn<ColumnString>(arguments[0].column.get()))
        {
            auto col_to = ColumnFixedString::create(SHA1_DIGEST_LEN);
            auto & chars_to = col_to->getChars();
            chars_to.resize(input_rows_count * SHA1_DIGEST_LEN);
            sha1BatchColumnString<Ops>(col_from->getChars(), col_from->getOffsets(), chars_to, input_rows_count);
            return col_to;
        }

        if (const auto * col_from_fix = checkAndGetColumn<ColumnFixedString>(arguments[0].column.get()))
        {
            auto col_to = ColumnFixedString::create(SHA1_DIGEST_LEN);
            auto & chars_to = col_to->getChars();
            chars_to.resize(input_rows_count * SHA1_DIGEST_LEN);
            sha1BatchFixedLen<Ops>(
                reinterpret_cast<const uint8_t *>(col_from_fix->getChars().data()), col_from_fix->getN(), chars_to, input_rows_count);
            return col_to;
        }

        if (const auto * col_from_ip = checkAndGetColumn<ColumnIPv6>(arguments[0].column.get()))
        {
            auto col_to = ColumnFixedString::create(SHA1_DIGEST_LEN);
            auto & chars_to = col_to->getChars();
            chars_to.resize(input_rows_count * SHA1_DIGEST_LEN);
            sha1BatchFixedLen<Ops>(
                reinterpret_cast<const uint8_t *>(col_from_ip->getData().data()), sizeof(IPv6::UnderlyingType), chars_to, input_rows_count);
            return col_to;
        }

        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function SHA1", arguments[0].column->getName());
    }

    ) // DECLARE_MULTITARGET_CODE

#undef SHA1_STEP
#undef SHA1_EXPAND
#undef SHA1_ROUND


/// Default path: use OpenSSL for hardware-accelerated SHA-1
/// (SHA-NI on x86, ARM Crypto Extensions on ARM, etc.)
#if USE_SSL

DECLARE_DEFAULT_CODE(

class FunctionSHA1Impl : public FunctionSHA1Base
{
public:
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        using EVP_MD_CTX_ptr = std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)>;

        /// Initialize a template context once. For each row we copy it via
        /// EVP_MD_CTX_copy_ex, which is much faster than EVP_DigestInit_ex
        /// per row (OpenSSL 3.x init goes through the provider layer).
        EVP_MD_CTX_ptr ctx_template(EVP_MD_CTX_new(), &EVP_MD_CTX_free);
        if (!ctx_template) [[unlikely]]
            throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_MD_CTX_new failed: {}", getOpenSSLErrors());

        if (EVP_DigestInit_ex(ctx_template.get(), EVP_sha1(), nullptr) != 1) [[unlikely]]
            throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_DigestInit_ex failed: {}", getOpenSSLErrors());

        EVP_MD_CTX_ptr ctx(EVP_MD_CTX_new(), &EVP_MD_CTX_free);
        if (!ctx) [[unlikely]]
            throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_MD_CTX_new failed: {}", getOpenSSLErrors());

        auto hash_one = [&](const uint8_t * data, size_t len, uint8_t * out)
        {
            if (EVP_MD_CTX_copy_ex(ctx.get(), ctx_template.get()) != 1) [[unlikely]]
                throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_MD_CTX_copy_ex failed: {}", getOpenSSLErrors());

            if (EVP_DigestUpdate(ctx.get(), data, len) != 1) [[unlikely]]
                throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_DigestUpdate failed: {}", getOpenSSLErrors());

            if (EVP_DigestFinal_ex(ctx.get(), out, nullptr) != 1) [[unlikely]]
                throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_DigestFinal_ex failed: {}", getOpenSSLErrors());
        };

        if (const auto * col_from = checkAndGetColumn<ColumnString>(arguments[0].column.get()))
        {
            auto col_to = ColumnFixedString::create(SHA1_DIGEST_LEN);
            auto & chars_to = col_to->getChars();
            chars_to.resize(input_rows_count * SHA1_DIGEST_LEN);

            const auto & data = col_from->getChars();
            const auto & offsets = col_from->getOffsets();
            ColumnString::Offset current_offset = 0;

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                hash_one(
                    reinterpret_cast<const uint8_t *>(&data[current_offset]),
                    offsets[i] - current_offset,
                    reinterpret_cast<uint8_t *>(&chars_to[i * SHA1_DIGEST_LEN]));
                current_offset = offsets[i];
            }
            return col_to;
        }

        if (const auto * col_from_fix = checkAndGetColumn<ColumnFixedString>(arguments[0].column.get()))
        {
            auto col_to = ColumnFixedString::create(SHA1_DIGEST_LEN);
            auto & chars_to = col_to->getChars();
            chars_to.resize(input_rows_count * SHA1_DIGEST_LEN);

            const auto & data = col_from_fix->getChars();
            size_t row_len = col_from_fix->getN();

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                hash_one(
                    reinterpret_cast<const uint8_t *>(&data[i * row_len]),
                    row_len,
                    reinterpret_cast<uint8_t *>(&chars_to[i * SHA1_DIGEST_LEN]));
            }
            return col_to;
        }

        if (const auto * col_from_ip = checkAndGetColumn<ColumnIPv6>(arguments[0].column.get()))
        {
            auto col_to = ColumnFixedString::create(SHA1_DIGEST_LEN);
            auto & chars_to = col_to->getChars();
            chars_to.resize(input_rows_count * SHA1_DIGEST_LEN);

            const auto & ip_data = col_from_ip->getData();

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                hash_one(
                    reinterpret_cast<const uint8_t *>(&ip_data[i]),
                    sizeof(IPv6::UnderlyingType),
                    reinterpret_cast<uint8_t *>(&chars_to[i * SHA1_DIGEST_LEN]));
            }
            return col_to;
        }

        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function SHA1", arguments[0].column->getName());
    }
};

) // DECLARE_DEFAULT_CODE

#else

DECLARE_DEFAULT_CODE(

class FunctionSHA1Impl : public FunctionSHA1Base
{
public:
    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t) const override
    {
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Function {} requires OpenSSL", name);
    }
};

) // DECLARE_DEFAULT_CODE

#endif


/// AVX-512 (16 lanes = 16 parallel digests)
DECLARE_X86_64_V4_SPECIFIC_CODE(

struct AVX512SHA1Ops
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

    static inline Vec xor_(Vec a, Vec b)
    {
        return _mm512_xor_si512(a, b);
    }
    /// 3-way XOR in a single instruction (used in message schedule expansion).
    static inline Vec xor3(Vec a, Vec b, Vec c)
    {
        return _mm512_ternarylogic_epi32(a, b, c, 0x96);
    }

    /// Ternary logic: single-instruction 3-input boolean functions.
    /// Ch(b, c, d) = d ^ (b & (c ^ d))
    static inline Vec F1(Vec b, Vec c, Vec d)
    {
        return _mm512_ternarylogic_epi32(b, c, d, 0xCA);
    }
    /// Parity(b, c, d) = b ^ c ^ d
    static inline Vec F2(Vec b, Vec c, Vec d)
    {
        return _mm512_ternarylogic_epi32(b, c, d, 0x96);
    }
    /// Maj(b, c, d) = (b & c) | (d & (b | c))
    static inline Vec F3(Vec b, Vec c, Vec d)
    {
        return _mm512_ternarylogic_epi32(b, c, d, 0xE8);
    }
    /// Parity(b, c, d) = b ^ c ^ d
    static inline Vec F4(Vec b, Vec c, Vec d)
    {
        return _mm512_ternarylogic_epi32(b, c, d, 0x96);
    }

    /// Extract lane `j` from a 512-bit vector using cross-lane permute
    /// (vpermd), avoiding the store-to-memory + scalar load round-trip.
    static inline uint32_t extract(Vec v, size_t j)
    {
        __m512i idx = _mm512_castsi128_si512(_mm_cvtsi32_si128(static_cast<int>(j)));
        return static_cast<uint32_t>(_mm512_cvtsi512_si32(_mm512_permutexvar_epi32(idx, v)));
    }

    /// Byte-swap mask for big-endian conversion within 32-bit words.
    static inline Vec bswapMask()
    {
        return _mm512_set4_epi32(
            static_cast<int>(0x0C0D0E0F), static_cast<int>(0x08090A0B), static_cast<int>(0x04050607), static_cast<int>(0x00010203));
    }

    /// 16x16 transpose then byte-swap for big-endian.
    static inline void gatherAllMessageWords(const uint8_t * const block_ptrs[], Vec msg[16])
    {
        Vec bswap = bswapMask();

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

        msg[0] = _mm512_shuffle_epi8(_mm512_shuffle_i32x4(v0, v8, 0x88), bswap);
        msg[4] = _mm512_shuffle_epi8(_mm512_shuffle_i32x4(v0, v8, 0xDD), bswap);
        msg[1] = _mm512_shuffle_epi8(_mm512_shuffle_i32x4(v2, v10, 0x88), bswap);
        msg[5] = _mm512_shuffle_epi8(_mm512_shuffle_i32x4(v2, v10, 0xDD), bswap);
        msg[2] = _mm512_shuffle_epi8(_mm512_shuffle_i32x4(v4, v12, 0x88), bswap);
        msg[6] = _mm512_shuffle_epi8(_mm512_shuffle_i32x4(v4, v12, 0xDD), bswap);
        msg[3] = _mm512_shuffle_epi8(_mm512_shuffle_i32x4(v6, v14, 0x88), bswap);
        msg[7] = _mm512_shuffle_epi8(_mm512_shuffle_i32x4(v6, v14, 0xDD), bswap);
        msg[8] = _mm512_shuffle_epi8(_mm512_shuffle_i32x4(v1, v9, 0x88), bswap);
        msg[12] = _mm512_shuffle_epi8(_mm512_shuffle_i32x4(v1, v9, 0xDD), bswap);
        msg[9] = _mm512_shuffle_epi8(_mm512_shuffle_i32x4(v3, v11, 0x88), bswap);
        msg[13] = _mm512_shuffle_epi8(_mm512_shuffle_i32x4(v3, v11, 0xDD), bswap);
        msg[10] = _mm512_shuffle_epi8(_mm512_shuffle_i32x4(v5, v13, 0x88), bswap);
        msg[14] = _mm512_shuffle_epi8(_mm512_shuffle_i32x4(v5, v13, 0xDD), bswap);
        msg[11] = _mm512_shuffle_epi8(_mm512_shuffle_i32x4(v7, v15, 0x88), bswap);
        msg[15] = _mm512_shuffle_epi8(_mm512_shuffle_i32x4(v7, v15, 0xDD), bswap);
    }
};

class FunctionSHA1Impl : public FunctionSHA1Base
{
public:
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        return executeSHA1Batch<AVX512SHA1Ops>(arguments, input_rows_count);
    }
};

) // DECLARE_X86_64_V4_SPECIFIC_CODE


#ifndef SHA1_GTEST_UNIT_TEST

/// Runtime dispatch via ImplementationSelector.
class FunctionSHA1 : public TargetSpecific::Default::FunctionSHA1Impl
{
public:
    explicit FunctionSHA1(ContextPtr context)
        : selector(context)
    {
#    if USE_SSL
        if (OpenSSLInitializer::instance().isFIPSEnabled())
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Function {} is not available in FIPS mode", name);
#    endif

        selector.registerImplementation<TargetArch::Default, TargetSpecific::Default::FunctionSHA1Impl>();

#    if USE_MULTITARGET_CODE
        selector.registerImplementation<TargetArch::x86_64_v4, TargetSpecific::x86_64_v4::FunctionSHA1Impl>();
#    endif
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return selector.selectAndExecute(arguments, result_type, input_rows_count);
    }

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionSHA1>(context); }

private:
    ImplementationSelector<IFunction> selector;
};


REGISTER_FUNCTION(SHA1)
{
    FunctionDocumentation::Description description_SHA1 = R"(
Calculates the SHA1 hash of the given string.
    )";
    FunctionDocumentation::Syntax syntax_SHA1 = "SHA1(s)";
    FunctionDocumentation::Arguments arguments_SHA1 = {{"s", "The input string to hash", {"String"}}};
    FunctionDocumentation::ReturnedValue returned_value_SHA1
        = {"Returns the SHA1 hash of the given input string as a fixed-length string.", {"FixedString(20)"}};
    FunctionDocumentation::Examples example_SHA1
        = {{"Usage example",
            R"(
SELECT HEX(SHA1('abc'));
        )",
            R"(
┌─hex(SHA1('abc'))─────────────────────────┐
│ A9993E364706816ABA3E25717850C26C9CD0D89D │
└──────────────────────────────────────────┘
        )"}};
    FunctionDocumentation::IntroducedIn introduced_in_SHA1 = {1, 1};
    FunctionDocumentation::Category category_SHA1 = FunctionDocumentation::Category::Hash;
    FunctionDocumentation documentation_SHA1
        = {description_SHA1, syntax_SHA1, arguments_SHA1, {}, returned_value_SHA1, example_SHA1, introduced_in_SHA1, category_SHA1};

    factory.registerFunction<FunctionSHA1>(documentation_SHA1);
}

#endif // SHA1_GTEST_UNIT_TEST
}
