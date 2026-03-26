#pragma once
#include <Common/TargetSpecific.h>

#include <Common/Base58.h>

#include <cstring>

#if USE_MULTITARGET_CODE
#    include <immintrin.h>
#endif

/// Firedancer based implementation as per https://github.com/firedancer-io/firedancer/tree/main/src/ballet/base58
/// Copyright (c) Firedancer contributors
/// Licensed under the Apache License 2.0
/// Adapted 2026 by jh0x (Joanna Hulboj)

namespace DB
{

constexpr uint8_t BASE58_INVALID_CHAR = 255;
constexpr uint8_t BASE58_INVERSE_TABLE_OFFSET = '1';
constexpr uint8_t BASE58_INVERSE_TABLE_SENTINEL = (1UL + ('z' - BASE58_INVERSE_TABLE_OFFSET));

constexpr char base58_chars[] = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

// clang-format off
#define BAD BASE58_INVALID_CHAR
constexpr uint8_t base58_inverse[] = {
    0, 1, 2, 3, 4, 5, 6, 7, 8, BAD,
    BAD, BAD, BAD, BAD, BAD, BAD, 9, 10, 11, 12,
    13, 14, 15, 16, BAD, 17, 18, 19, 20, 21,
    BAD, 22, 23, 24, 25, 26, 27, 28, 29, 30,
    31, 32, BAD, BAD, BAD, BAD, BAD, BAD, 33, 34,
    35, 36, 37, 38, 39, 40, 41, 42, 43, BAD,
    44, 45, 46, 47, 48, 49, 50, 51, 52, 53,
    54, 55, 56, 57, BAD
};
#undef BAD

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

constexpr uint32_t dec_table_32[9][8] = {
    {      1277U, 2650397687U, 3801011509U, 2074386530U, 3248244966U,  687255411U, 2959155456U,          0U},
    {         0U,       8360U, 1184754854U, 3047609191U, 3418394749U,  132556120U, 1199103528U,          0U},
    {         0U,          0U,      54706U, 2996985344U, 1834629191U, 3964963911U,  485140318U, 1073741824U},
    {         0U,          0U,          0U,     357981U, 1476998812U, 3337178590U, 1483338760U, 4194304000U},
    {         0U,          0U,          0U,          0U,    2342503U, 3052466824U, 2595180627U,   17825792U},
    {         0U,          0U,          0U,          0U,          0U,   15328518U, 1933902296U, 4063920128U},
    {         0U,          0U,          0U,          0U,          0U,          0U,  100304420U, 3355157504U},
    {         0U,          0U,          0U,          0U,          0U,          0U,          0U,  656356768U},
    {         0U,          0U,          0U,          0U,          0U,          0U,          0U,          1U}
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

constexpr uint32_t dec_table_64[18][16] = {
    {    249448U, 3719864065U,  173911550U, 4021557284U, 3115810883U, 2498525019U, 1035889824U,  627529458U, 3840888383U, 3728167192U, 2901437456U, 3863405776U, 1540739182U, 1570766848U,          0U,          0U},
    {         0U,    1632305U, 1882780341U, 4128706713U, 1023671068U, 2618421812U, 2005415586U, 1062993857U, 3577221846U, 3960476767U, 1695615427U, 2597060712U,  669472826U,  104923136U,          0U,          0U},
    {         0U,          0U,   10681231U, 1422956801U, 2406345166U, 4058671871U, 2143913881U, 4169135587U, 2414104418U, 2549553452U,  997594232U,  713340517U, 2290070198U, 1103833088U,          0U,          0U},
    {         0U,          0U,          0U,   69894212U, 1038812943U, 1785020643U, 1285619000U, 2301468615U, 3492037905U,  314610629U, 2761740102U, 3410618104U, 1699516363U,  910779968U,          0U,          0U},
    {         0U,          0U,          0U,          0U,  457363084U,  927569770U, 3976106370U, 1389513021U, 2107865525U, 3716679421U, 1828091393U, 2088408376U,  439156799U, 2579227194U,          0U,          0U},
    {         0U,          0U,          0U,          0U,          0U, 2992822783U,  383623235U, 3862831115U,  112778334U,  339767049U, 1447250220U,  486575164U, 3495303162U, 2209946163U,  268435456U,          0U},
    {         0U,          0U,          0U,          0U,          0U,          4U, 2404108010U, 2962826229U, 3998086794U, 1893006839U, 2266258239U, 1429430446U,  307953032U, 2361423716U,  176160768U,          0U},
    {         0U,          0U,          0U,          0U,          0U,          0U,         29U, 3596590989U, 3044036677U, 1332209423U, 1014420882U,  868688145U, 4264082837U, 3688771808U, 2485387264U,          0U},
    {         0U,          0U,          0U,          0U,          0U,          0U,          0U,        195U, 1054003707U, 3711696540U,  582574436U, 3549229270U, 1088536814U, 2338440092U, 1468637184U,          0U},
    {         0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,       1277U, 2650397687U, 3801011509U, 2074386530U, 3248244966U,  687255411U, 2959155456U,          0U},
    {         0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,       8360U, 1184754854U, 3047609191U, 3418394749U,  132556120U, 1199103528U,          0U},
    {         0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,      54706U, 2996985344U, 1834629191U, 3964963911U,  485140318U, 1073741824U},
    {         0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,     357981U, 1476998812U, 3337178590U, 1483338760U, 4194304000U},
    {         0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,    2342503U, 3052466824U, 2595180627U,   17825792U},
    {         0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,   15328518U, 1933902296U, 4063920128U},
    {         0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,  100304420U, 3355157504U},
    {         0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,  656356768U},
    {         0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,          0U,          1U}
};
// clang-format on

static inline uint32_t b58_bswap32(uint32_t x)
{
    return __builtin_bswap32(x);
}

static inline uint32_t b58_load_u32_be(const uint8_t * p)
{
    uint32_t v;
    memcpy(&v, p, 4);
    return b58_bswap32(v);
}

static inline int b58_find_lsb(uint64_t x)
{
    /// returns index of lowest set bit; UB if x==0
    return __builtin_ctzll(x);
}

static inline int b58_find_lsb_default(uint64_t x, int def)
{
    return x ? b58_find_lsb(x) : def;
}

DECLARE_DEFAULT_CODE(
static inline size_t encodeBase58_32(const uint8_t * src, uint8_t * dst)
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

static size_t encodeBase58_64(const uint8_t * src, uint8_t * dst)
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

static std::optional<size_t> decodeBase58_32(const uint8_t * src, size_t src_length, uint8_t * dst)
{
    constexpr size_t BINARY_SZ = 8;
    constexpr size_t INTERMEDIATE_SZ = 9;
    constexpr size_t RAW58_SZ = INTERMEDIATE_SZ * 5;
    constexpr size_t BYTE_CNT = 32;

    /// Minimum encoded length is BYTE_CNT (all-zero input -> BYTE_CNT leading '1's).
    if (src_length < BYTE_CNT || src_length > BASE58_ENCODED_32_LEN)
        return std::nullopt;

    for (size_t i = 0; i < src_length; i++)
    {
        size_t idx = static_cast<size_t>(static_cast<uint8_t>(src[i])) - static_cast<size_t>(BASE58_INVERSE_TABLE_OFFSET);
        if (idx > BASE58_INVERSE_TABLE_SENTINEL)
            return std::nullopt;
        if (base58_inverse[idx] == BASE58_INVALID_CHAR)
            return std::nullopt;
    }

    uint8_t raw[RAW58_SZ];
    size_t prepend_0 = RAW58_SZ - src_length;
    for (size_t j = 0; j < RAW58_SZ; j++)
        raw[j] = (j < prepend_0) ? 0 : base58_inverse[static_cast<uint8_t>(src[j - prepend_0]) - BASE58_INVERSE_TABLE_OFFSET];

    uint64_t intermediate[INTERMEDIATE_SZ];
    for (size_t i = 0; i < INTERMEDIATE_SZ; i++)
        intermediate[i] = static_cast<uint64_t>(raw[5 * i + 0]) * 11316496ULL + static_cast<uint64_t>(raw[5 * i + 1]) * 195112ULL
            + static_cast<uint64_t>(raw[5 * i + 2]) * 3364ULL + static_cast<uint64_t>(raw[5 * i + 3]) * 58ULL
            + static_cast<uint64_t>(raw[5 * i + 4]);

    uint64_t binary[BINARY_SZ] = {};
    for (size_t j = 0; j < BINARY_SZ; j++)
        for (size_t i = 0; i < INTERMEDIATE_SZ; i++)
            binary[j] += static_cast<uint64_t>(intermediate[i]) * static_cast<uint64_t>(dec_table_32[i][j]);

    for (size_t i = BINARY_SZ - 1; i > 0; i--)
    {
        binary[i - 1] += binary[i] >> 32;
        binary[i] &= 0xFFFFFFFFULL;
    }

    /// If binary[0] overflows 32 bits the value exceeds 2^(BYTE_CNT*8);
    /// reject so the caller falls back to the universal decoder.
    if (binary[0] > 0xFFFFFFFFULL)
        return std::nullopt;

    for (size_t i = 0; i < BINARY_SZ; i++)
    {
        uint32_t word_be = b58_bswap32(static_cast<uint32_t>(binary[i]));
        memcpy(dst + 4 * i, &word_be, sizeof(word_be));
    }

    size_t leading_zero_cnt = 0;
    for (; leading_zero_cnt < BYTE_CNT; leading_zero_cnt++)
    {
        if (dst[leading_zero_cnt])
            break;
        if (static_cast<uint8_t>(src[leading_zero_cnt]) != static_cast<uint8_t>('1'))
            return std::nullopt;
    }
    if (leading_zero_cnt < src_length && src[leading_zero_cnt] == static_cast<uint8_t>('1'))
        return std::nullopt;

    return BYTE_CNT;
}

static std::optional<size_t> decodeBase58_64(const uint8_t * src, size_t src_length, uint8_t * dst)
{
    constexpr size_t BINARY_SZ = 16;
    constexpr size_t INTERMEDIATE_SZ = 18;
    constexpr size_t RAW58_SZ = INTERMEDIATE_SZ * 5;
    constexpr size_t BYTE_CNT = 64;

    /// Minimum encoded length is BYTE_CNT (all-zero input -> BYTE_CNT leading '1's).
    if (src_length < BYTE_CNT || src_length > BASE58_ENCODED_64_LEN)
        return std::nullopt;

    for (size_t i = 0; i < src_length; i++)
    {
        size_t idx = static_cast<size_t>(static_cast<uint8_t>(src[i])) - static_cast<size_t>(BASE58_INVERSE_TABLE_OFFSET);
        if (idx > BASE58_INVERSE_TABLE_SENTINEL)
            return std::nullopt;
        if (base58_inverse[idx] == BASE58_INVALID_CHAR)
            return std::nullopt;
    }

    uint8_t raw[RAW58_SZ];
    size_t prepend_0 = RAW58_SZ - src_length;
    for (size_t j = 0; j < RAW58_SZ; j++)
        raw[j] = (j < prepend_0) ? 0 : base58_inverse[static_cast<uint8_t>(src[j - prepend_0]) - BASE58_INVERSE_TABLE_OFFSET];

    uint64_t intermediate[INTERMEDIATE_SZ];
    for (size_t i = 0; i < INTERMEDIATE_SZ; i++)
        intermediate[i] = static_cast<uint64_t>(raw[5 * i + 0]) * 11316496ULL + static_cast<uint64_t>(raw[5 * i + 1]) * 195112ULL
            + static_cast<uint64_t>(raw[5 * i + 2]) * 3364ULL + static_cast<uint64_t>(raw[5 * i + 3]) * 58ULL
            + static_cast<uint64_t>(raw[5 * i + 4]);

    uint64_t binary[BINARY_SZ] = {};
    for (size_t j = 0; j < BINARY_SZ; j++)
        for (size_t i = 0; i < INTERMEDIATE_SZ; i++)
            binary[j] += static_cast<uint64_t>(intermediate[i]) * static_cast<uint64_t>(dec_table_64[i][j]);

    for (size_t i = BINARY_SZ - 1; i > 0; i--)
    {
        binary[i - 1] += binary[i] >> 32;
        binary[i] &= 0xFFFFFFFFULL;
    }

    /// If binary[0] overflows 32 bits the value exceeds 2^(BYTE_CNT*8);
    /// reject so the caller falls back to the universal decoder.
    if (binary[0] > 0xFFFFFFFFULL)
        return std::nullopt;

    for (size_t i = 0; i < BINARY_SZ; i++)
    {
        uint32_t word_be = b58_bswap32(static_cast<uint32_t>(binary[i]));
        memcpy(dst + 4 * i, &word_be, sizeof(word_be));
    }

    size_t leading_zero_cnt = 0;
    for (; leading_zero_cnt < BYTE_CNT; leading_zero_cnt++)
    {
        if (dst[leading_zero_cnt])
            break;
        if (static_cast<uint8_t>(src[leading_zero_cnt]) != static_cast<uint8_t>('1'))
            return std::nullopt;
    }
    if (leading_zero_cnt < src_length && src[leading_zero_cnt] == static_cast<uint8_t>('1'))
        return std::nullopt;

    return BYTE_CNT;
}
)

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


#if USE_MULTITARGET_CODE
DECLARE_X86_64_V3_SPECIFIC_CODE(
/// ---- AVX helpers (replacing firedancer wl_t / wuc_t abstractions) ---------
static inline __m256i b58_intermediate_to_raw(__m256i intermediate)
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

static inline __m256i b58_raw_to_base58(__m256i in)
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

static inline uint64_t b58_count_leading_zeros_26(__m256i in)
{
    uint64_t mask0 = static_cast<uint64_t>(static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(in, _mm256_setzero_si256()))));
    uint64_t mask = ((1ULL << 27) - 1ULL) ^ (mask0 & ((1ULL << 26) - 1ULL));
    return static_cast<uint64_t>(b58_find_lsb(mask));
}

static inline uint64_t b58_count_leading_zeros_32(__m256i in)
{
    uint64_t mask = ((1ULL << 33) - 1ULL)
        ^ static_cast<uint64_t>(static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(in, _mm256_setzero_si256()))));
    return static_cast<uint64_t>(b58_find_lsb(mask));
}

static inline uint64_t b58_count_leading_zeros_45(__m256i in0, __m256i in1)
{
    uint64_t mask0 = static_cast<uint64_t>(static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(in0, _mm256_setzero_si256()))));
    uint64_t mask1 = static_cast<uint64_t>(static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(in1, _mm256_setzero_si256()))));
    uint64_t mask = ((1ULL << 46) - 1ULL) ^ (((mask1 & ((1ULL << 13) - 1ULL)) << 32) | mask0);
    return static_cast<uint64_t>(b58_find_lsb(mask));
}

static inline uint64_t b58_count_leading_zeros_64(__m256i in0, __m256i in1)
{
    uint64_t mask0 = static_cast<uint64_t>(static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(in0, _mm256_setzero_si256()))));
    uint64_t mask1 = static_cast<uint64_t>(static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(in1, _mm256_setzero_si256()))));
    uint64_t mask = ~((mask1 << 32) | mask0);
    return static_cast<uint64_t>(b58_find_lsb_default(mask, 64));
}

static size_t encodeBase58_32(const uint8_t * src, uint8_t * dst)
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
    /// so RAW58_SZ (45) - skip ≤ 44 = BASE58_ENCODED_32_LEN.
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

static size_t encodeBase58_64(const uint8_t * src, uint8_t * dst)
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
    /// so RAW58_SZ (90) - skip ≤ 88 = BASE58_ENCODED_64_LEN.
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
    static constexpr uint64_t MAX_SKIP_64 = 26;
    alignas(32) uint8_t scratch[MAX_SKIP_64 + RAW58_SZ];
    uint8_t * scratch_dst = scratch + MAX_SKIP_64;
    auto scratch_addr = reinterpret_cast<uintptr_t>(scratch_dst);

    _mm256_maskstore_epi64(reinterpret_cast<long long *>(scratch_addr - 8ULL * (skip / 8ULL)), mask1, shifted);
    _mm256_maskstore_epi64(reinterpret_cast<long long *>(scratch_addr - skip), mask2, base58_0);
    _mm256_storeu_si256(reinterpret_cast<__m256i *>(scratch_dst + 32ULL - skip), base58_1);

    __m128i last = _mm_bslli_si128(_mm256_extractf128_si256(base58_2, 1), 6);
    _mm_storeu_si128(reinterpret_cast<__m128i *>(scratch_dst + 74ULL - skip), last);
    _mm_storeu_si128(reinterpret_cast<__m128i *>(scratch_dst + 64ULL - skip), _mm256_extractf128_si256(base58_2, 0));

    size_t len = RAW58_SZ - skip;
    memcpy(dst, scratch_dst, len);
    return len;
})
#endif

}
