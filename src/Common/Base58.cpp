#include <Common/Base58.h>

#include <base/unaligned.h>
#include <base/wide_integer.h>

#include <algorithm>
#include <bit>
#include <cstring>
#include <limits>
#include <optional>

#if defined(__AVX2__)
#    include <immintrin.h>
#endif

/// Firedancer based implementation as per https://github.com/firedancer-io/firedancer/tree/main/src/ballet/base58
/// Copyright (c) Firedancer contributors
/// Licensed under the Apache License 2.0
/// Adapted 2026 by jh0x (Joanna Hulboj)

namespace DB
{

namespace
{

// clang-format off
constexpr uint32_t enc_table_32[8][8] = {
    {   513735U,  77223048U, 437087610U, 300156666U, 605448490U, 214625350U, 141436834U, 379377856U},
    {        0U,     78508U, 646269101U, 118408823U,  91512303U, 209184527U, 413102373U, 153715680U},
    {        0U,         0U,     11997U, 486083817U,   3737691U, 294005210U, 247894721U, 289024608U},
    {        0U,         0U,         0U,      1833U, 324463681U, 385795061U, 551597588U,  21339008U},
    {        0U,         0U,         0U,         0U,       280U, 127692781U, 389432875U, 357132832U},
    {        0U,         0U,         0U,         0U,         0U,        42U, 537767569U, 410450016U},
    {        0U,         0U,         0U,         0U,         0U,         0U,         6U, 356826688U},
    {        0U,         0U,         0U,         0U,         0U,         0U,         0U,         1U}
};

constexpr uint32_t enc_table_64[16][17] = {
    {     2631U, 149457141U, 577092685U, 632289089U,  81912456U, 221591423U, 502967496U, 403284731U, 377738089U, 492128779U,    746799U, 366351977U, 190199623U,  38066284U, 526403762U, 650603058U, 454901440U},
    {        0U,       402U,  68350375U,  30641941U, 266024478U, 208884256U, 571208415U, 337765723U, 215140626U, 129419325U, 480359048U, 398051646U, 635841659U, 214020719U, 136986618U, 626219915U,  49699360U},
    {        0U,         0U,        61U, 295059608U, 141201404U, 517024870U, 239296485U, 527697587U, 212906911U, 453637228U, 467589845U, 144614682U,  45134568U, 184514320U, 644355351U, 104784612U, 308625792U},
    {        0U,         0U,         0U,         9U, 256449755U, 500124311U, 479690581U, 372802935U, 413254725U, 487877412U, 520263169U, 176791855U,  78190744U, 291820402U,  74998585U, 496097732U,  59100544U},
    {        0U,         0U,         0U,         0U,         1U, 285573662U, 455976778U, 379818553U, 100001224U, 448949512U, 109507367U, 117185012U, 347328982U, 522665809U,  36908802U, 577276849U,  64504928U},
    {        0U,         0U,         0U,         0U,         0U,         0U, 143945778U, 651677945U, 281429047U, 535878743U, 264290972U, 526964023U, 199595821U, 597442702U, 499113091U, 424550935U, 458949280U},
    {        0U,         0U,         0U,         0U,         0U,         0U,         0U,  21997789U, 294590275U, 148640294U, 595017589U, 210481832U, 404203788U, 574729546U, 160126051U, 430102516U,  44963712U},
    {        0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,   3361701U, 325788598U,  30977630U, 513969330U, 194569730U, 164019635U, 136596846U, 626087230U, 503769920U},
    {        0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,    513735U,  77223048U, 437087610U, 300156666U, 605448490U, 214625350U, 141436834U, 379377856U},
    {        0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,     78508U, 646269101U, 118408823U,  91512303U, 209184527U, 413102373U, 153715680U},
    {        0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,     11997U, 486083817U,   3737691U, 294005210U, 247894721U, 289024608U},
    {        0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,      1833U, 324463681U, 385795061U, 551597588U,  21339008U},
    {        0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,       280U, 127692781U, 389432875U, 357132832U},
    {        0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,        42U, 537767569U, 410450016U},
    {        0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,         6U, 356826688U},
    {        0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,         0U,         1U}
};
// clang-format on

/// The codec treats each 4-byte group of the value as a big-endian word, independently of the
/// host's own byte order.
inline uint32_t b58_load_u32_be(const uint8_t * p)
{
    uint32_t v = unalignedLoad<uint32_t>(p);
    if constexpr (std::endian::native == std::endian::little)
        v = std::byteswap(v);
    return v;
}

#if !defined(__AVX2__)
constexpr char base58_chars[] = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

size_t encodeBase58_32_fd(const uint8_t * src, uint8_t * dst)
{
    constexpr size_t BINARY_SZ = 8;
    constexpr size_t INTERMEDIATE_SZ = 9;
    constexpr size_t RAW58_SZ = INTERMEDIATE_SZ * 5;

    size_t in_leading_0s = 0;
    for (; in_leading_0s < 32; in_leading_0s++)
        if (src[in_leading_0s])
            break;

    uint32_t binary[BINARY_SZ];
    for (size_t i = 0; i < BINARY_SZ; i++)
        binary[i] = b58_load_u32_be(src + i * 4);

    uint64_t intermediate[INTERMEDIATE_SZ] = {};
    for (size_t i = 0; i < BINARY_SZ; i++)
        for (size_t j = 0; j < INTERMEDIATE_SZ - 1; j++)
            intermediate[j + 1] += static_cast<uint64_t>(binary[i]) * static_cast<uint64_t>(enc_table_32[i][j]);

    const uint64_t R1div = 656356768ULL; // 58^5
    for (size_t i = INTERMEDIATE_SZ - 1; i > 0; i--)
    {
        intermediate[i - 1] += intermediate[i] / R1div;
        intermediate[i] %= R1div;
    }

    uint8_t raw[RAW58_SZ];
    for (size_t i = 0; i < INTERMEDIATE_SZ; i++)
    {
        uint32_t v = static_cast<uint32_t>(intermediate[i]);
        raw[5 * i + 4] = static_cast<uint8_t>((v / 1U) % 58U);
        raw[5 * i + 3] = static_cast<uint8_t>((v / 58U) % 58U);
        raw[5 * i + 2] = static_cast<uint8_t>((v / 3364U) % 58U);
        raw[5 * i + 1] = static_cast<uint8_t>((v / 195112U) % 58U);
        raw[5 * i + 0] = static_cast<uint8_t>(v / 11316496U);
    }

    size_t raw_leading_0s = 0;
    for (; raw_leading_0s < RAW58_SZ; raw_leading_0s++)
        if (raw[raw_leading_0s])
            break;

    size_t skip = raw_leading_0s - in_leading_0s;
    size_t out_len = RAW58_SZ - skip;
    for (size_t i = 0; i < out_len; i++)
        dst[i] = static_cast<uint8_t>(base58_chars[raw[skip + i]]);
    return out_len;
}

size_t encodeBase58_64_fd(const uint8_t * src, uint8_t * dst)
{
    constexpr size_t BINARY_SZ = 16;
    constexpr size_t INTERMEDIATE_SZ = 18;
    constexpr size_t RAW58_SZ = INTERMEDIATE_SZ * 5;

    size_t in_leading_0s = 0;
    for (; in_leading_0s < 64; in_leading_0s++)
        if (src[in_leading_0s])
            break;

    uint32_t binary[BINARY_SZ];
    for (size_t i = 0; i < BINARY_SZ; i++)
        binary[i] = b58_load_u32_be(src + i * 4);

    uint64_t intermediate[INTERMEDIATE_SZ] = {};
    const uint64_t R1div = 656356768ULL;

    for (size_t i = 0; i < 8; i++)
        for (size_t j = 0; j < INTERMEDIATE_SZ - 1; j++)
            intermediate[j + 1] += static_cast<uint64_t>(binary[i]) * static_cast<uint64_t>(enc_table_64[i][j]);
    /// mini-reduction to avoid overflow
    intermediate[15] += intermediate[16] / R1div;
    intermediate[16] %= R1div;
    for (size_t i = 8; i < BINARY_SZ; i++)
        for (size_t j = 0; j < INTERMEDIATE_SZ - 1; j++)
            intermediate[j + 1] += static_cast<uint64_t>(binary[i]) * static_cast<uint64_t>(enc_table_64[i][j]);

    for (size_t i = INTERMEDIATE_SZ - 1; i > 0; i--)
    {
        intermediate[i - 1] += intermediate[i] / R1div;
        intermediate[i] %= R1div;
    }

    uint8_t raw[RAW58_SZ];
    for (size_t i = 0; i < INTERMEDIATE_SZ; i++)
    {
        uint32_t v = static_cast<uint32_t>(intermediate[i]);
        raw[5 * i + 4] = static_cast<uint8_t>((v / 1U) % 58U);
        raw[5 * i + 3] = static_cast<uint8_t>((v / 58U) % 58U);
        raw[5 * i + 2] = static_cast<uint8_t>((v / 3364U) % 58U);
        raw[5 * i + 1] = static_cast<uint8_t>((v / 195112U) % 58U);
        raw[5 * i + 0] = static_cast<uint8_t>(v / 11316496U);
    }

    size_t raw_leading_0s = 0;
    for (; raw_leading_0s < RAW58_SZ; raw_leading_0s++)
        if (raw[raw_leading_0s])
            break;

    size_t skip = raw_leading_0s - in_leading_0s;
    size_t out_len = RAW58_SZ - skip;
    for (size_t i = 0; i < out_len; i++)
        dst[i] = static_cast<uint8_t>(base58_chars[raw[skip + i]]);
    return out_len;
}
#endif // !defined(__AVX2__)

#if defined(__AVX2__)

inline int b58_find_lsb(uint64_t x)
{
    /// returns index of lowest set bit; UB if x==0
    return __builtin_ctzll(x);
}

inline int b58_find_lsb_default(uint64_t x, int def)
{
    return x ? b58_find_lsb(x) : def;
}

#define B58_TEN_PER_SLOT_DOWN_32(in0, in1, in2, out0, out1) \
    do \
    { \
        __m128i lo0 = _mm256_extractf128_si256(in0, 0); \
        __m128i hi0 = _mm256_extractf128_si256(in0, 1); \
        __m128i lo1 = _mm256_extractf128_si256(in1, 0); \
        __m128i hi1 = _mm256_extractf128_si256(in1, 1); \
        __m128i lo2 = _mm256_extractf128_si256(in2, 0); \
        __m128i o0 = _mm_or_si128(lo0, _mm_slli_si128(hi0, 10)); \
        __m128i o1 = _mm_or_si128(_mm_or_si128(_mm_srli_si128(hi0, 6), _mm_slli_si128(lo1, 4)), _mm_slli_si128(hi1, 14)); \
        __m128i o2 = _mm_or_si128(_mm_srli_si128(hi1, 2), _mm_slli_si128(lo2, 8)); \
        (out0) = _mm256_set_m128i(o1, o0); \
        (out1) = _mm256_set_m128i(_mm_setzero_si128(), o2); \
    } while (0)

#define B58_TEN_PER_SLOT_DOWN_64(in0, in1, in2, in3, in4, out0, out1, out2) \
    do \
    { \
        __m128i lo0 = _mm256_extractf128_si256(in0, 0); \
        __m128i hi0 = _mm256_extractf128_si256(in0, 1); \
        __m128i lo1 = _mm256_extractf128_si256(in1, 0); \
        __m128i hi1 = _mm256_extractf128_si256(in1, 1); \
        __m128i lo2 = _mm256_extractf128_si256(in2, 0); \
        __m128i hi2 = _mm256_extractf128_si256(in2, 1); \
        __m128i lo3 = _mm256_extractf128_si256(in3, 0); \
        __m128i hi3 = _mm256_extractf128_si256(in3, 1); \
        __m128i lo4 = _mm256_extractf128_si256(in4, 0); \
        __m128i o0 = _mm_or_si128(lo0, _mm_slli_si128(hi0, 10)); \
        __m128i o1 = _mm_or_si128(_mm_or_si128(_mm_srli_si128(hi0, 6), _mm_slli_si128(lo1, 4)), _mm_slli_si128(hi1, 14)); \
        __m128i o2 = _mm_or_si128(_mm_srli_si128(hi1, 2), _mm_slli_si128(lo2, 8)); \
        __m128i o3 = _mm_or_si128(_mm_or_si128(_mm_srli_si128(lo2, 8), _mm_slli_si128(hi2, 2)), _mm_slli_si128(lo3, 12)); \
        __m128i o4 = _mm_or_si128(_mm_srli_si128(lo3, 4), _mm_slli_si128(hi3, 6)); \
        (out0) = _mm256_set_m128i(o1, o0); \
        (out1) = _mm256_set_m128i(o3, o2); \
        (out2) = _mm256_set_m128i(lo4, o4); \
    } while (0)

/// ---- AVX helpers (replacing firedancer wl_t / wuc_t abstractions) ---------
inline __m256i b58_intermediate_to_raw(__m256i intermediate)
{
    __m256i cA = _mm256_set1_epi64x(static_cast<long long>(2369637129U)); // 2^37/58
    __m256i cB = _mm256_set1_epi64x(static_cast<long long>(1307386003U)); // 2^42/58^2
    __m256i _58 = _mm256_set1_epi64x(58LL);

    /// div(k) = floor(x / 58^k), rem(k) = div(k) % 58
    __m256i div0 = intermediate;
    __m256i div1 = _mm256_srli_epi64(_mm256_mul_epu32(div0, cA), 37);
    __m256i rem0 = _mm256_sub_epi64(div0, _mm256_mul_epu32(div1, _58));

    __m256i div2 = _mm256_srli_epi64(_mm256_mul_epu32(_mm256_srli_epi64(div0, 2), cB), 40);
    __m256i rem1 = _mm256_sub_epi64(div1, _mm256_mul_epu32(div2, _58));

    __m256i div3 = _mm256_srli_epi64(_mm256_mul_epu32(_mm256_srli_epi64(div1, 2), cB), 40);
    __m256i rem2 = _mm256_sub_epi64(div2, _mm256_mul_epu32(div3, _58));

    __m256i div4 = _mm256_srli_epi64(_mm256_mul_epu32(_mm256_srli_epi64(div2, 2), cB), 40);
    __m256i rem3 = _mm256_sub_epi64(div3, _mm256_mul_epu32(div4, _58));

    __m256i rem4 = div4;

    __m256i shuffle1 = _mm256_setr_epi8(0, 1, 1, 1, 1, 8, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 8, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);

    __m256i shift4 = _mm256_shuffle_epi8(rem4, shuffle1);
    __m256i shift3 = _mm256_slli_si256(_mm256_shuffle_epi8(rem3, shuffle1), 1);
    __m256i shift2 = _mm256_slli_si256(_mm256_shuffle_epi8(rem2, shuffle1), 2);
    __m256i shift1 = _mm256_slli_si256(_mm256_shuffle_epi8(rem1, shuffle1), 3);
    __m256i shift0 = _mm256_slli_si256(_mm256_shuffle_epi8(rem0, shuffle1), 4);

    return _mm256_or_si256(_mm256_or_si256(_mm256_or_si256(shift4, shift3), _mm256_or_si256(shift2, shift1)), shift0);
}

inline __m256i b58_raw_to_base58(__m256i in)
{
    /// Maps each byte in [0, 58) to its base58 ASCII character via:
    ///   b58ch(x) = '1' + x + 7*[x>8] + [x>16] + [x>21] + 6*[x>32] + [x>43]
    /// where [cond] is 1 if true, 0 if false (Knuth bracket notation).
    /// cmpgt_epi8 returns 0xFF (= -1) for true, so AND with -7 gives {0,-7},
    /// which we subtract to effectively add 7.
    __m256i gt0 = _mm256_cmpgt_epi8(in, _mm256_set1_epi8(8));
    __m256i gt1 = _mm256_cmpgt_epi8(in, _mm256_set1_epi8(16));
    __m256i gt2 = _mm256_cmpgt_epi8(in, _mm256_set1_epi8(21));
    __m256i gt3 = _mm256_cmpgt_epi8(in, _mm256_set1_epi8(32));
    __m256i gt4 = _mm256_cmpgt_epi8(in, _mm256_set1_epi8(43));

    __m256i gt0_7 = _mm256_and_si256(gt0, _mm256_set1_epi8(-7));
    __m256i gt3_6 = _mm256_and_si256(gt3, _mm256_set1_epi8(-6));

    __m256i sum = _mm256_add_epi8(
        _mm256_add_epi8(_mm256_add_epi8(_mm256_set1_epi8(-static_cast<int8_t>('1')), gt1), _mm256_add_epi8(gt2, gt4)),
        _mm256_add_epi8(gt0_7, gt3_6));

    return _mm256_sub_epi8(in, sum);
}

inline uint64_t b58_count_leading_zeros_26(__m256i in)
{
    uint64_t mask0 = static_cast<uint64_t>(static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(in, _mm256_setzero_si256()))));
    uint64_t mask = ((1ULL << 27) - 1ULL) ^ (mask0 & ((1ULL << 26) - 1ULL));
    return static_cast<uint64_t>(b58_find_lsb(mask));
}

inline uint64_t b58_count_leading_zeros_32(__m256i in)
{
    uint64_t mask = ((1ULL << 33) - 1ULL)
        ^ static_cast<uint64_t>(static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(in, _mm256_setzero_si256()))));
    return static_cast<uint64_t>(b58_find_lsb(mask));
}

inline uint64_t b58_count_leading_zeros_45(__m256i in0, __m256i in1)
{
    uint64_t mask0 = static_cast<uint64_t>(static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(in0, _mm256_setzero_si256()))));
    uint64_t mask1 = static_cast<uint64_t>(static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(in1, _mm256_setzero_si256()))));
    uint64_t mask = ((1ULL << 46) - 1ULL) ^ (((mask1 & ((1ULL << 13) - 1ULL)) << 32) | mask0);
    return static_cast<uint64_t>(b58_find_lsb(mask));
}

inline uint64_t b58_count_leading_zeros_64(__m256i in0, __m256i in1)
{
    uint64_t mask0 = static_cast<uint64_t>(static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(in0, _mm256_setzero_si256()))));
    uint64_t mask1 = static_cast<uint64_t>(static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(in1, _mm256_setzero_si256()))));
    uint64_t mask = ~((mask1 << 32) | mask0);
    return static_cast<uint64_t>(b58_find_lsb_default(mask, 64));
}

size_t encodeBase58_32_fd(const uint8_t * src, uint8_t * dst)
{
    constexpr size_t BINARY_SZ = 8;
    constexpr size_t INTERMEDIATE_SZ = 9;
    constexpr size_t INTERMEDIATE_SZ_PAD = 12; // align up to 4
    constexpr size_t RAW58_SZ = INTERMEDIATE_SZ * 5;

    __m256i _bytes = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(src));
    uint64_t in_leading_0s = b58_count_leading_zeros_32(_bytes);

    uint32_t binary[BINARY_SZ];
    for (size_t i = 0; i < BINARY_SZ; i++)
        binary[i] = b58_load_u32_be(src + i * 4);

    alignas(32) uint64_t intermediate[INTERMEDIATE_SZ_PAD] = {};
    const uint64_t R1div = 656356768ULL;

    for (size_t i = 0; i < BINARY_SZ; i++)
        for (size_t j = 0; j < INTERMEDIATE_SZ - 1; j++)
            intermediate[j + 1] += static_cast<uint64_t>(binary[i]) * static_cast<uint64_t>(enc_table_32[i][j]);
    for (size_t i = INTERMEDIATE_SZ - 1; i > 0; i--)
    {
        intermediate[i - 1] += intermediate[i] / R1div;
        intermediate[i] %= R1div;
    }

    __m256i interm0 = _mm256_load_si256(reinterpret_cast<const __m256i *>(intermediate));
    __m256i interm1 = _mm256_load_si256(reinterpret_cast<const __m256i *>(intermediate + 4));
    __m256i interm2 = _mm256_load_si256(reinterpret_cast<const __m256i *>(intermediate + 8));

    __m256i raw0 = b58_intermediate_to_raw(interm0);
    __m256i raw1 = b58_intermediate_to_raw(interm1);
    __m256i raw2 = b58_intermediate_to_raw(interm2);

    __m256i compact0;
    __m256i compact1;
    B58_TEN_PER_SLOT_DOWN_32(raw0, raw1, raw2, compact0, compact1);

    uint64_t raw_leading_0s = b58_count_leading_zeros_45(compact0, compact1);

    __m256i base58_0 = b58_raw_to_base58(compact0);
    __m256i base58_1 = b58_raw_to_base58(compact1);

    /// skip in [1, 13]: the final string is between 32 and 44 characters,
    /// so RAW58_SZ (45) - skip <= 44 = BASE58_ENCODED_32_LEN.
    uint64_t skip = raw_leading_0s - in_leading_0s;

    __m256i w_skip = _mm256_set1_epi64x(static_cast<long long>(skip));
    __m256i mod8_mask = _mm256_set1_epi64x(7LL);
    __m256i compare = _mm256_set_epi64x(3LL, 2LL, 1LL, 0LL);

    __m256i shift_qty = _mm256_slli_epi64(_mm256_and_si256(w_skip, mod8_mask), 3);
    __m256i shifted = _mm256_srlv_epi64(base58_0, shift_qty);
    __m256i skip_div8 = _mm256_srli_epi64(w_skip, 3);

    __m256i mask1 = _mm256_cmpeq_epi64(skip_div8, compare);
    __m256i mask2 = _mm256_cmpgt_epi64(compare, skip_div8);

    /// Stage stores into a scratch buffer with front-padding so that
    /// (scratch_dst - skip) never forms an out-of-bounds pointer.
    static constexpr uint64_t MAX_SKIP_32 = 13;
    alignas(32) uint8_t scratch[MAX_SKIP_32 + RAW58_SZ];
    uint8_t * scratch_dst = scratch + MAX_SKIP_32;
    auto scratch_addr = reinterpret_cast<uintptr_t>(scratch_dst);

    _mm256_maskstore_epi64(reinterpret_cast<long long *>(scratch_addr - 8ULL * (skip / 8ULL)), mask1, shifted);

    __m128i last = _mm_bslli_si128(_mm256_extractf128_si256(base58_1, 0), 3);
    _mm_storeu_si128(reinterpret_cast<__m128i *>(scratch_dst + 29ULL - skip), last);

    _mm256_maskstore_epi64(reinterpret_cast<long long *>(scratch_addr - skip), mask2, base58_0);

    size_t len = RAW58_SZ - skip;
    memcpy(dst, scratch_dst, len);
    return len;
}

size_t encodeBase58_64_fd(const uint8_t * src, uint8_t * dst)
{
    constexpr size_t BINARY_SZ = 16;
    constexpr size_t INTERMEDIATE_SZ = 18;
    constexpr size_t INTERMEDIATE_SZ_PAD = 20; // align up to 4
    constexpr size_t RAW58_SZ = INTERMEDIATE_SZ * 5;

    __m256i bytes_0 = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(src));
    __m256i bytes_1 = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(src + 32));
    uint64_t in_leading_0s = b58_count_leading_zeros_64(bytes_0, bytes_1);

    uint32_t binary[BINARY_SZ];
    for (size_t i = 0; i < BINARY_SZ; i++)
        binary[i] = b58_load_u32_be(src + i * 4);

    alignas(32) uint64_t intermediate[INTERMEDIATE_SZ_PAD] = {};
    const uint64_t R1div = 656356768ULL;

    for (size_t i = 0; i < 8; i++)
        for (size_t j = 0; j < INTERMEDIATE_SZ - 1; j++)
            intermediate[j + 1] += static_cast<uint64_t>(binary[i]) * static_cast<uint64_t>(enc_table_64[i][j]);
    intermediate[15] += intermediate[16] / R1div;
    intermediate[16] %= R1div;
    for (size_t i = 8; i < BINARY_SZ; i++)
        for (size_t j = 0; j < INTERMEDIATE_SZ - 1; j++)
            intermediate[j + 1] += static_cast<uint64_t>(binary[i]) * static_cast<uint64_t>(enc_table_64[i][j]);
    for (size_t i = INTERMEDIATE_SZ - 1; i > 0; i--)
    {
        intermediate[i - 1] += intermediate[i] / R1div;
        intermediate[i] %= R1div;
    }

    __m256i raw0 = b58_intermediate_to_raw(_mm256_load_si256(reinterpret_cast<const __m256i *>(intermediate)));
    __m256i raw1 = b58_intermediate_to_raw(_mm256_load_si256(reinterpret_cast<const __m256i *>(intermediate + 4)));
    __m256i raw2 = b58_intermediate_to_raw(_mm256_load_si256(reinterpret_cast<const __m256i *>(intermediate + 8)));
    __m256i raw3 = b58_intermediate_to_raw(_mm256_load_si256(reinterpret_cast<const __m256i *>(intermediate + 12)));
    __m256i raw4 = b58_intermediate_to_raw(_mm256_load_si256(reinterpret_cast<const __m256i *>(intermediate + 16)));

    __m256i compact0;
    __m256i compact1;
    __m256i compact2;
    B58_TEN_PER_SLOT_DOWN_64(raw0, raw1, raw2, raw3, raw4, compact0, compact1, compact2);

    uint64_t raw_leading_0s_part1 = b58_count_leading_zeros_64(compact0, compact1);
    uint64_t raw_leading_0s_part2 = b58_count_leading_zeros_26(compact2);
    uint64_t raw_leading_0s = (raw_leading_0s_part1 < 64ULL) ? raw_leading_0s_part1 : 64ULL + raw_leading_0s_part2;

    __m256i base58_0 = b58_raw_to_base58(compact0);
    __m256i base58_1 = b58_raw_to_base58(compact1);
    __m256i base58_2 = b58_raw_to_base58(compact2);

    /// skip in [2, 26]: the final string is between 64 and 88 characters,
    /// so RAW58_SZ (90) - skip <= 88 = BASE58_ENCODED_64_LEN.
    uint64_t skip = raw_leading_0s - in_leading_0s;

    __m256i w_skip = _mm256_set1_epi64x(static_cast<long long>(skip));
    __m256i mod8_mask = _mm256_set1_epi64x(7LL);
    __m256i compare = _mm256_set_epi64x(3LL, 2LL, 1LL, 0LL);

    __m256i shift_qty = _mm256_slli_epi64(_mm256_and_si256(w_skip, mod8_mask), 3);
    __m256i shifted = _mm256_srlv_epi64(base58_0, shift_qty);
    __m256i skip_div8 = _mm256_srli_epi64(w_skip, 3);

    __m256i mask1_v = _mm256_cmpeq_epi64(skip_div8, compare);
    __m256i mask2_v = _mm256_cmpgt_epi64(compare, skip_div8);

    /// Stage stores into a scratch buffer with front-padding so that
    /// (scratch_dst - skip) never forms an out-of-bounds pointer.
    static constexpr uint64_t MAX_SKIP_64 = 26;
    alignas(32) uint8_t scratch[MAX_SKIP_64 + RAW58_SZ];
    uint8_t * scratch_dst = scratch + MAX_SKIP_64;
    auto scratch_addr = reinterpret_cast<uintptr_t>(scratch_dst);

    _mm256_maskstore_epi64(reinterpret_cast<long long *>(scratch_addr - 8ULL * (skip / 8ULL)), mask1_v, shifted);
    _mm256_maskstore_epi64(reinterpret_cast<long long *>(scratch_addr - skip), mask2_v, base58_0);
    _mm256_storeu_si256(reinterpret_cast<__m256i *>(scratch_dst + 32ULL - skip), base58_1);

    __m128i last = _mm_bslli_si128(_mm256_extractf128_si256(base58_2, 1), 6);
    _mm_storeu_si128(reinterpret_cast<__m128i *>(scratch_dst + 74ULL - skip), last);
    _mm_storeu_si128(reinterpret_cast<__m128i *>(scratch_dst + 64ULL - skip), _mm256_extractf128_si256(base58_2, 0));

    size_t len = RAW58_SZ - skip;
    memcpy(dst, scratch_dst, len);
    return len;
}
#endif // defined(__AVX2__)

} // anonymous namespace

namespace
{

constexpr UInt64 power58(size_t exponent)
{
    UInt64 result = 1;
    for (size_t i = 0; i < exponent; ++i)
        result *= 58;
    return result;
}

/// 58^10 is the largest power of 58 in a `UInt64`, which is what makes it the word radix.
constexpr size_t BASE58_ENCODE_WORD_DIGITS = 10;
constexpr UInt64 BASE58_ENCODE_WORD_RADIX = power58(BASE58_ENCODE_WORD_DIGITS);
constexpr wide::invariant_divisor BASE58_ENCODE_WORD_DIVISOR = wide::prepare_divisor(BASE58_ENCODE_WORD_RADIX);

/// Input elements per outer pass, `n` below. Encode takes 8 bytes, not 9: the dividend `word * 2^(8 * n)
/// + carry` must fit `UInt128`, and `carry` itself a `UInt64`, so `8 * n` must stay within 64. Decode
/// takes 10 characters, not 11: `58^n` must fit `UInt64`, and 58^10 does while 58^11 does not.
constexpr size_t BASE58_ENCODE_BYTES_PER_PASS = 8;
constexpr size_t BASE58_DECODE_CHARS_PER_PASS = 10;
constexpr UInt64 BASE58_DECODE_PASS_MULTIPLIER = power58(BASE58_DECODE_CHARS_PER_PASS);

static_assert(BASE58_ENCODE_WORD_RADIX > std::numeric_limits<UInt64>::max() / 58);
static_assert(8 * BASE58_ENCODE_BYTES_PER_PASS <= 64);

/// Upper bounds on the word count: 1366/1000 exceeds 8/log2(58) and 733/1000 exceeds log2(58)/8.
constexpr size_t base58EncodeWords(size_t body)
{
    return (body * 1366 / 1000 + 2 + BASE58_ENCODE_WORD_DIGITS - 1) / BASE58_ENCODE_WORD_DIGITS;
}

constexpr size_t base58DecodeWords(size_t body)
{
    return (body * 733 / 1000 + 2 + sizeof(UInt64) - 1) / sizeof(UInt64);
}

/// The words live in the destination buffer when they fit its documented bound (2n+1 encode, n decode).
/// A decode below 16 characters can need two words, 16 bytes, which is more than `dst` holds, so there an
/// array is required rather than merely preferred. The cutoffs below are not that point; they are the
/// largest bodies whose word-count bound fits 64, which is why they cover both cases.
constexpr size_t BASE58_STACK_WORDS = 64;
constexpr size_t BASE58_ENCODE_STACK_MAX_BODY = 467;
constexpr size_t BASE58_DECODE_STACK_MAX_BODY = 697;

static_assert(base58EncodeWords(BASE58_ENCODE_STACK_MAX_BODY) <= BASE58_STACK_WORDS);
static_assert(base58EncodeWords(BASE58_ENCODE_STACK_MAX_BODY + 1) > BASE58_STACK_WORDS);
static_assert(base58DecodeWords(BASE58_DECODE_STACK_MAX_BODY) <= BASE58_STACK_WORDS);
static_assert(base58DecodeWords(BASE58_DECODE_STACK_MAX_BODY + 1) > BASE58_STACK_WORDS);

/// The short-path bounds. Eight bytes is the largest body a `UInt64` holds; eleven characters is the
/// encoded length of eight bytes, and the only length that can exceed one, since 58^10 <= 2^64 - 1 < 58^11.
constexpr size_t BASE58_SHORT_ENCODE_MAX_BODY = sizeof(UInt64);
constexpr size_t BASE58_SHORT_DECODE_MAX_BODY = 11;

static_assert(power58(BASE58_SHORT_DECODE_MAX_BODY - 1) <= std::numeric_limits<UInt64>::max());
static_assert(power58(BASE58_SHORT_DECODE_MAX_BODY - 1) > std::numeric_limits<UInt64>::max() / 58);

/// `decodeBase58` hands the word array's bytes back as its own output, so the layout is part of the
/// algorithm and is little-endian on every host, not just the ones where that is the native order.
/// Unaligned because the words may live in a byte buffer.
UInt64 loadWord(const UInt8 * words, size_t i)
{
    return unalignedLoadLittleEndian<UInt64>(words + i * sizeof(UInt64));
}

void storeWord(UInt8 * words, size_t i, UInt64 value)
{
    unalignedStoreLittleEndian<UInt64>(words + i * sizeof(UInt64), value);
}


/// A body that fits a `UInt64` needs none of the word apparatus above: the conversion is repeated divmod
/// in a register. The digits are written least significant first and then reversed through the alphabet
/// in place, exactly as the general path does, so at most 11 digits land inside the `2 * body + 1` bound.
size_t encodeBase58Short(const UInt8 * src, size_t body_length, UInt8 * dst, const char * alphabet)
{
    UInt64 value = 0;
    for (size_t i = 0; i < body_length; ++i)
        value = (value << 8) | src[i];

    size_t idx = 0;
    while (value > 0)
    {
        const UInt64 quotient = value / 58;
        dst[idx] = static_cast<UInt8>(value - quotient * 58);
        ++idx;
        value = quotient;
    }

    size_t c_idx = idx >> 1;
    for (size_t i = 0; i < c_idx; ++i)
    {
        char s = alphabet[dst[i]];
        dst[i] = alphabet[dst[idx - (i + 1)]];
        dst[idx - (i + 1)] = s;
    }

    if ((idx & 1))
        dst[c_idx] = alphabet[dst[c_idx]];

    return idx;
}

/// The mirror of `encodeBase58Short`. An empty result means this path does not handle the input - an
/// invalid character, or eleven characters above a `UInt64` - and nothing has been written to `dst`, so
/// the caller falls through to the general path, which handles both.
std::optional<size_t> decodeBase58Short(const UInt8 * src, size_t body_length, UInt8 * dst, const Int8 * map_digits)
{
    const size_t always_fits = body_length < BASE58_SHORT_DECODE_MAX_BODY ? body_length : BASE58_SHORT_DECODE_MAX_BODY - 1;
    UInt64 value = 0;
    for (size_t i = 0; i < always_fits; ++i)
    {
        const Int8 digit = map_digits[src[i]];
        if (digit < 0)
            return {};
        value = value * 58 + static_cast<UInt64>(digit);
    }

    if (body_length == BASE58_SHORT_DECODE_MAX_BODY)
    {
        const Int8 digit = map_digits[src[BASE58_SHORT_DECODE_MAX_BODY - 1]];
        if (digit < 0)
            return {};
        const UInt64 last = static_cast<UInt64>(digit);
        if (value > (std::numeric_limits<UInt64>::max() - last) / 58)
            return {};
        value = value * 58 + last;
    }

    /// Least significant byte first, extracted arithmetically, so the result does not depend on how the
    /// host stores a `UInt64`. The general path leaves its own output in that order too, hence the reversal.
    size_t idx = 0;
    while (value > 0)
    {
        dst[idx] = static_cast<UInt8>(value & 0xFF);
        ++idx;
        value >>= 8;
    }

    std::reverse(dst, dst + idx);
    return idx;
}

} // anonymous namespace


size_t encodeBase58(const UInt8 * src, size_t src_length, UInt8 * dst, const std::function<void()> & check_cancellation)
{
    const char * base58_encoding_alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

    size_t processed = 0;
    size_t idx = 0;
    size_t zeros = 0;

    while (processed < src_length && *src == 0)
    {
        ++processed;
        ++zeros;
        *dst = '1';
        ++dst;
        ++src;
    }

    const size_t body_length = src_length - processed;

    if (body_length <= BASE58_SHORT_ENCODE_MAX_BODY)
        return zeros + encodeBase58Short(src, body_length, dst, base58_encoding_alphabet);

    /// The accumulator is the input so far as `UInt64` words of radix 58^10, least significant word first.
    UInt64 stack_words[BASE58_STACK_WORDS];
    UInt8 * words = body_length > BASE58_ENCODE_STACK_MAX_BODY ? dst : reinterpret_cast<UInt8 *>(stack_words);
    size_t word_count = 0;

    /// The total work is quadratic, so the cancellation check is driven by accumulated inner-loop work
    /// rather than by outer iterations: the time limit and `KILL QUERY` stay prompt even with the size
    /// limit disabled. The unit counted is one (input element, accumulator element) pair, and one
    /// iteration covers `BASE58_ENCODE_BYTES_PER_PASS * BASE58_ENCODE_WORD_DIGITS` of them, hence the scaling.
    size_t work_since_check = 0;
    static constexpr size_t work_per_check = 1ULL << 20;

    /// A short leading pass goes first, so every pass below reads exactly `BASE58_ENCODE_BYTES_PER_PASS` bytes.
    if (size_t head = body_length % BASE58_ENCODE_BYTES_PER_PASS)
    {
        UInt64 carry = 0;
        for (size_t i = 0; i < head; ++i)
            carry = (carry << 8) | src[i];
        src += head;
        processed += head;

        /// At most seven bytes, so below 2^56 and therefore below the radix: one word holds them.
        if (carry > 0)
        {
            storeWord(words, word_count, carry);
            ++word_count;
        }
    }

    while (processed < src_length)
    {
        if (check_cancellation)
        {
            work_since_check += word_count * BASE58_ENCODE_BYTES_PER_PASS * BASE58_ENCODE_WORD_DIGITS;
            if (work_since_check >= work_per_check)
            {
                check_cancellation();
                work_since_check = 0;
            }
        }

        UInt64 carry = 0;
        for (size_t i = 0; i < BASE58_ENCODE_BYTES_PER_PASS; ++i)
            carry = (carry << 8) | src[i];
        src += BASE58_ENCODE_BYTES_PER_PASS;
        processed += BASE58_ENCODE_BYTES_PER_PASS;

        /// `word < radix` is the accumulator's invariant, and it is also what makes each step a
        /// division whose quotient fits one word. `carry` is not below the radix on the first step,
        /// where it is the raw input bytes, and does not need to be.
        for (size_t j = 0; j < word_count; ++j)
        {
            UInt64 remainder = 0;
            const UInt64 quotient
                = wide::divide_128_by_64(loadWord(words, j), carry, BASE58_ENCODE_WORD_DIVISOR, remainder);
            storeWord(words, j, remainder);
            carry = quotient;
        }

        while (carry > 0)
        {
            storeWord(words, word_count, carry % BASE58_ENCODE_WORD_RADIX);
            ++word_count;
            carry /= BASE58_ENCODE_WORD_RADIX;
        }
    }

    /// The most significant word is never zero: multiplying a non-zero accumulator by 2^64 always carries
    /// past one radix-58^10 word, so a pass that would zero the top word appends a non-zero one above it.
    /// Expanding top-down, reading each word before writing over it, is what keeps this safe while the
    /// words are IN `dst`: words 0 .. i-1 occupy `dst[0, 8 * i)` and word i's digits start at 10 * i.
    if (word_count)
    {
        UInt64 top = loadWord(words, word_count - 1);
        const size_t top_base = BASE58_ENCODE_WORD_DIGITS * (word_count - 1);
        size_t top_digits = 0;
        do
        {
            const UInt64 quotient = top / 58;
            dst[top_base + top_digits] = static_cast<UInt8>(top - quotient * 58);
            top = quotient;
            ++top_digits;
        } while (top > 0);
        idx = top_base + top_digits;

        for (size_t i = word_count - 1; i-- > 0;)
        {
            UInt64 word = loadWord(words, i);
            for (size_t d = 0; d < BASE58_ENCODE_WORD_DIGITS; ++d)
            {
                const UInt64 quotient = word / 58;
                dst[BASE58_ENCODE_WORD_DIGITS * i + d] = static_cast<UInt8>(word - quotient * 58);
                word = quotient;
            }
        }
    }

    size_t c_idx = idx >> 1;
    for (size_t i = 0; i < c_idx; ++i)
    {
        char s = base58_encoding_alphabet[dst[i]];
        dst[i] = base58_encoding_alphabet[dst[idx - (i + 1)]];
        dst[idx - (i + 1)] = s;
    }

    if ((idx & 1))
    {
        dst[c_idx] = base58_encoding_alphabet[dst[c_idx]];
    }

    return zeros + idx;
}


std::optional<size_t> decodeBase58(const UInt8 * src, size_t src_length, UInt8 * dst, const std::function<void()> & check_cancellation)
{
    // clang-format off
    static const Int8 map_digits[256] =
    {
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1,  0,  1,  2,  3,  4,  5,  6,  7,  8, -1, -1, -1, -1, -1, -1,
        -1,  9, 10, 11, 12, 13, 14, 15, 16, -1, 17, 18, 19, 20, 21, -1,
        22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, -1, -1, -1, -1, -1,
        -1, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, -1, 44, 45, 46,
        47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    };
    // clang-format on

    size_t processed = 0;
    size_t idx = 0;
    size_t zeros = 0;

    while (processed < src_length && *src == '1')
    {
        ++processed;
        ++zeros;
        *dst = '\0';
        ++dst;
        ++src;
    }

    const size_t body_length = src_length - processed;

    if (body_length <= BASE58_SHORT_DECODE_MAX_BODY)
    {
        if (const std::optional<size_t> bytes = decodeBase58Short(src, body_length, dst, map_digits))
            return zeros + *bytes;
    }

    /// The accumulator is the characters so far as `UInt64` words of base 2^64, least significant first -
    /// byte for byte the little-endian form the result needs, so only the byte reversal is left at the end.
    UInt64 stack_words[BASE58_STACK_WORDS];
    UInt8 * words = body_length > BASE58_DECODE_STACK_MAX_BODY ? dst : reinterpret_cast<UInt8 *>(stack_words);
    size_t word_count = 0;

    /// As in `encodeBase58`, the check is driven by accumulated inner-loop work, and one iteration
    /// covers `BASE58_DECODE_CHARS_PER_PASS * sizeof(UInt64)` of the pairs that unit counts.
    size_t work_since_check = 0;
    static constexpr size_t work_per_check = 1ULL << 20;

    /// A short leading pass goes first, so every pass below reads exactly `BASE58_DECODE_CHARS_PER_PASS` characters.
    if (size_t head = body_length % BASE58_DECODE_CHARS_PER_PASS)
    {
        UInt64 carry = 0;
        for (size_t i = 0; i < head; ++i)
        {
            const Int8 digit = map_digits[src[i]];
            if (digit < 0)
                return {};
            carry = carry * 58 + static_cast<UInt64>(digit);
        }
        src += head;
        processed += head;

        /// At most nine characters, so below 58^9 and therefore below 2^64: one word holds them.
        if (carry > 0)
        {
            storeWord(words, word_count, carry);
            ++word_count;
        }
    }

    while (processed < src_length)
    {
        if (check_cancellation)
        {
            work_since_check += word_count * BASE58_DECODE_CHARS_PER_PASS * sizeof(UInt64);
            if (work_since_check >= work_per_check)
            {
                check_cancellation();
                work_since_check = 0;
            }
        }

        UInt64 carry = 0;
        for (size_t i = 0; i < BASE58_DECODE_CHARS_PER_PASS; ++i)
        {
            const Int8 digit = map_digits[src[i]];
            if (digit < 0)
                return {};
            carry = carry * 58 + static_cast<UInt64>(digit);
        }
        src += BASE58_DECODE_CHARS_PER_PASS;
        processed += BASE58_DECODE_CHARS_PER_PASS;

        for (size_t j = 0; j < word_count; ++j)
        {
            const unsigned __int128 cur
                = static_cast<unsigned __int128>(loadWord(words, j)) * BASE58_DECODE_PASS_MULTIPLIER + carry;
            storeWord(words, j, static_cast<UInt64>(cur));
            carry = static_cast<UInt64>(cur >> 64);
        }

        /// The carry out of a 128-bit product is one word wide, so at most one word is appended.
        if (carry > 0)
        {
            storeWord(words, word_count, carry);
            ++word_count;
        }
    }

    /// The most significant word is never zero: a pass rewrites a top word `t >= 1` as
    /// `t * 58^n + carry >= 58^n`, so it either stays non-zero in place or carries, and the
    /// last word the append loop above stores is the final non-zero `carry`.
    if (word_count)
    {
        size_t top_bytes = 1;
        for (UInt64 rest = loadWord(words, word_count - 1) >> 8; rest; rest >>= 8)
            ++top_bytes;
        idx = sizeof(UInt64) * (word_count - 1) + top_bytes;
        if (words != dst)
            memcpy(dst, words, idx);
    }

    size_t c_idx = idx >> 1;
    for (size_t i = 0; i < c_idx; ++i)
    {
        UInt8 s = dst[i];
        dst[i] = dst[idx - (i + 1)];
        dst[idx - (i + 1)] = s;
    }

    return zeros + idx;
}

size_t encodeBase58_32(const UInt8 * src, UInt8 * dst)
{
    return encodeBase58_32_fd(reinterpret_cast<const uint8_t *>(src), reinterpret_cast<uint8_t *>(dst));
}

size_t encodeBase58_64(const UInt8 * src, UInt8 * dst)
{
    return encodeBase58_64_fd(reinterpret_cast<const uint8_t *>(src), reinterpret_cast<uint8_t *>(dst));
}

}
