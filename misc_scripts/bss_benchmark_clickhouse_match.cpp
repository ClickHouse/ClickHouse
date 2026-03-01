//
// Benchmark for ByteStreamSplit codec — mirrors the exact ClickHouse implementations.
// Includes correctness tests AND security/robustness validation for the decode path.
//
// HEADER FORMAT (v2 — 5 bytes):
//   [0..3]  int32_t  element_bytes_size   (little-endian)
//   [4]     uint8_t  bytes_to_skip        (legacy; recomputed on decode)
//

#include <cstdint>
#include <cstring>
#include <cstdio>
#include <chrono>
#include <vector>
#include <algorithm>
#include <cassert>
#include <stdexcept>

#ifdef __x86_64__
#include <immintrin.h>
#endif

using Clock = std::chrono::high_resolution_clock;

// Maximum element width — matches ClickHouse (UInt8 max, enforced at registration)
static constexpr int MAX_ELEMENT_WIDTH = 255;

// Header size: 4 bytes (int32 element width) + 1 byte (bytes_to_skip)
static constexpr uint32_t HEADER_SIZE = 5;

// =============================================================================
//  W=16 SIMD — SSE2 butterfly transpose (shared by encode + decode)
// =============================================================================

#ifdef __x86_64__

/// 16×16 byte matrix transpose via SSE2 unpack butterfly.
/// See ClickHouse source for full stage commentary.
__attribute__((target("sse2")))
static inline void butterfly16(const __m128i R[16], __m128i V[16])
{
    __m128i T[16];
    for (int k = 0; k < 8; ++k)
    {
        T[2 * k]     = _mm_unpacklo_epi8(R[2 * k], R[2 * k + 1]);
        T[2 * k + 1] = _mm_unpackhi_epi8(R[2 * k], R[2 * k + 1]);
    }

    __m128i U[16];
    U[0]  = _mm_unpacklo_epi16(T[0],  T[2]);
    U[1]  = _mm_unpackhi_epi16(T[0],  T[2]);
    U[2]  = _mm_unpacklo_epi16(T[4],  T[6]);
    U[3]  = _mm_unpackhi_epi16(T[4],  T[6]);
    U[4]  = _mm_unpacklo_epi16(T[1],  T[3]);
    U[5]  = _mm_unpackhi_epi16(T[1],  T[3]);
    U[6]  = _mm_unpacklo_epi16(T[5],  T[7]);
    U[7]  = _mm_unpackhi_epi16(T[5],  T[7]);
    U[8]  = _mm_unpacklo_epi16(T[8],  T[10]);
    U[9]  = _mm_unpackhi_epi16(T[8],  T[10]);
    U[10] = _mm_unpacklo_epi16(T[12], T[14]);
    U[11] = _mm_unpackhi_epi16(T[12], T[14]);
    U[12] = _mm_unpacklo_epi16(T[9],  T[11]);
    U[13] = _mm_unpackhi_epi16(T[9],  T[11]);
    U[14] = _mm_unpacklo_epi16(T[13], T[15]);
    U[15] = _mm_unpackhi_epi16(T[13], T[15]);

    V[0]  = _mm_unpacklo_epi32(U[0],  U[2]);
    V[1]  = _mm_unpackhi_epi32(U[0],  U[2]);
    V[2]  = _mm_unpacklo_epi32(U[1],  U[3]);
    V[3]  = _mm_unpackhi_epi32(U[1],  U[3]);
    V[4]  = _mm_unpacklo_epi32(U[4],  U[6]);
    V[5]  = _mm_unpackhi_epi32(U[4],  U[6]);
    V[6]  = _mm_unpacklo_epi32(U[5],  U[7]);
    V[7]  = _mm_unpackhi_epi32(U[5],  U[7]);
    V[8]  = _mm_unpacklo_epi32(U[8],  U[10]);
    V[9]  = _mm_unpackhi_epi32(U[8],  U[10]);
    V[10] = _mm_unpacklo_epi32(U[9],  U[11]);
    V[11] = _mm_unpackhi_epi32(U[9],  U[11]);
    V[12] = _mm_unpacklo_epi32(U[12], U[14]);
    V[13] = _mm_unpackhi_epi32(U[12], U[14]);
    V[14] = _mm_unpacklo_epi32(U[13], U[15]);
    V[15] = _mm_unpackhi_epi32(U[13], U[15]);
}

__attribute__((target("sse2")))
static inline void storeRows(const __m128i V[16], uint8_t * base)
{
    for (int k = 0; k < 8; ++k)
    {
        _mm_storeu_si128(reinterpret_cast<__m128i *>(base + (2 * k)     * 16), _mm_unpacklo_epi64(V[k], V[k + 8]));
        _mm_storeu_si128(reinterpret_cast<__m128i *>(base + (2 * k + 1) * 16), _mm_unpackhi_epi64(V[k], V[k + 8]));
    }
}

__attribute__((target("sse2")))
static inline void storeBands(const __m128i V[16], uint8_t * w[16], int64_t col)
{
    for (int k = 0; k < 8; ++k)
    {
        _mm_storeu_si128(reinterpret_cast<__m128i *>(w[2 * k]     + col), _mm_unpacklo_epi64(V[k], V[k + 8]));
        _mm_storeu_si128(reinterpret_cast<__m128i *>(w[2 * k + 1] + col), _mm_unpackhi_epi64(V[k], V[k + 8]));
    }
}

// =============================================================================
//  AVX2 encode W=16: 4-stage splitEvenOddBytes deinterleave
// =============================================================================

__attribute__((target("avx2")))
static inline void splitEvenOddBytes(
    __m256i a, __m256i b,
    __m256i & even_out, __m256i & odd_out,
    __m256i mask)
{
    __m256i a_even = _mm256_and_si256(a, mask);
    __m256i a_odd  = _mm256_srli_epi16(a, 8);
    __m256i b_even = _mm256_and_si256(b, mask);
    __m256i b_odd  = _mm256_srli_epi16(b, 8);
    even_out = _mm256_permute4x64_epi64(_mm256_packus_epi16(a_even, b_even), 0xD8);
    odd_out  = _mm256_permute4x64_epi64(_mm256_packus_epi16(a_odd,  b_odd),  0xD8);
}

__attribute__((target("avx2")))
static void encode16_AVX2(
    const uint8_t * __restrict__ src,
    uint8_t       * __restrict__ dst,
    int64_t N)
{
    const __m256i mask = _mm256_set1_epi16(0x00FF);

    int64_t i = 0;
    for (; i + 32 <= N; i += 32)
    {
        const uint8_t * s = src + i * 16;

        __m256i r[16];
        for (int k = 0; k < 16; ++k)
            r[k] = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(s + k * 32));

        __m256i s1[16];
        for (int k = 0; k < 8; ++k)
            splitEvenOddBytes(r[2 * k], r[2 * k + 1], s1[k], s1[k + 8], mask);

        __m256i s2[16];
        for (int k = 0; k < 4; ++k)
            splitEvenOddBytes(s1[2 * k], s1[2 * k + 1], s2[k], s2[k + 4], mask);
        for (int k = 0; k < 4; ++k)
            splitEvenOddBytes(s1[8 + 2 * k], s1[8 + 2 * k + 1], s2[8 + k], s2[8 + k + 4], mask);

        __m256i s3[16];
        for (int k = 0; k < 2; ++k)
            splitEvenOddBytes(s2[2 * k], s2[2 * k + 1], s3[k], s3[k + 2], mask);
        for (int k = 0; k < 2; ++k)
            splitEvenOddBytes(s2[4 + 2 * k], s2[4 + 2 * k + 1], s3[4 + k], s3[4 + k + 2], mask);
        for (int k = 0; k < 2; ++k)
            splitEvenOddBytes(s2[8 + 2 * k], s2[8 + 2 * k + 1], s3[8 + k], s3[8 + k + 2], mask);
        for (int k = 0; k < 2; ++k)
            splitEvenOddBytes(s2[12 + 2 * k], s2[12 + 2 * k + 1], s3[12 + k], s3[12 + k + 2], mask);

        __m256i out[16];
        for (int g = 0; g < 8; ++g)
            splitEvenOddBytes(s3[2 * g], s3[2 * g + 1], out[g], out[g + 8], mask);

        static const int band_of[16] = {0, 4, 2, 6, 1, 5, 3, 7, 8, 12, 10, 14, 9, 13, 11, 15};
        for (int k = 0; k < 16; ++k)
            _mm256_storeu_si256(reinterpret_cast<__m256i *>(dst + static_cast<int64_t>(band_of[k]) * N + i), out[k]);
    }
    _mm256_zeroupper();

    for (; i < N; ++i)
        for (int b = 0; b < 16; ++b)
            dst[static_cast<int64_t>(b) * N + i] = src[i * 16 + b];
}

// =============================================================================
//  SSE2 encode W=16
// =============================================================================

__attribute__((target("sse2")))
static void encode16_SSE2(
    const uint8_t * __restrict__ src,
    uint8_t       * __restrict__ dst,
    int64_t N)
{
    int64_t i = 0;
    for (; i + 16 <= N; i += 16)
    {
        __m128i R[16], V[16];
        for (int k = 0; k < 16; ++k)
            R[k] = _mm_loadu_si128(reinterpret_cast<const __m128i *>(src + (i + k) * 16));
        butterfly16(R, V);

        uint8_t * w[16];
        for (int b = 0; b < 16; ++b)
            w[b] = dst + static_cast<int64_t>(b) * N;
        storeBands(V, w, i);
    }
    for (; i < N; ++i)
        for (int b = 0; b < 16; ++b)
            dst[static_cast<int64_t>(b) * N + i] = src[i * 16 + b];
}

// =============================================================================
//  AVX2 decode W=16
// =============================================================================

__attribute__((target("avx2")))
static void decode16_AVX2(
    const uint8_t * __restrict__ src,
    uint8_t       * __restrict__ dst,
    int64_t N)
{
    const uint8_t * r[16];
    for (int b = 0; b < 16; ++b)
        r[b] = src + static_cast<int64_t>(b) * N;

    int64_t i = 0;
    for (; i + 32 <= N; i += 32)
    {
        __m128i Lo[16], Hi[16], VL[16], VH[16];
        for (int b = 0; b < 16; ++b)
        {
            __m256i tmp = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(r[b] + i));
            Lo[b] = _mm256_castsi256_si128(tmp);
            Hi[b] = _mm256_extracti128_si256(tmp, 1);
        }
        butterfly16(Lo, VL);
        storeRows(VL, dst + i * 16);
        butterfly16(Hi, VH);
        storeRows(VH, dst + (i + 16) * 16);
    }
    _mm256_zeroupper();

    for (; i < N; ++i)
        for (int b = 0; b < 16; ++b)
            dst[i * 16 + b] = r[b][i];
}

// =============================================================================
//  SSE2 decode W=16
// =============================================================================

__attribute__((target("sse2")))
static void decode16_SSE2(
    const uint8_t * __restrict__ src,
    uint8_t       * __restrict__ dst,
    int64_t N)
{
    const uint8_t * r[16];
    for (int b = 0; b < 16; ++b)
        r[b] = src + static_cast<int64_t>(b) * N;

    int64_t i = 0;
    for (; i + 16 <= N; i += 16)
    {
        __m128i R[16], V[16];
        for (int b = 0; b < 16; ++b)
            R[b] = _mm_loadu_si128(reinterpret_cast<const __m128i *>(r[b] + i));
        butterfly16(R, V);
        storeRows(V, dst + i * 16);
    }
    for (; i < N; ++i)
        for (int b = 0; b < 16; ++b)
            dst[i * 16 + b] = r[b][i];
}

#endif // __x86_64__


// =============================================================================
//  Encode implementations (raw transpose — no header)
// =============================================================================

template <int W>
__attribute__((always_inline)) inline
void encodeW(
    const uint8_t * __restrict__ src,
    uint8_t       * __restrict__ dst,
    int64_t num_elements)
{
    for (int64_t i = 0; i < num_elements; ++i)
        for (int64_t b = 0; b < W; ++b)
            dst[b * num_elements + i] = src[i * W + b];
}

template <>
__attribute__((always_inline)) inline
void encodeW<16>(
    const uint8_t * __restrict__ src,
    uint8_t       * __restrict__ dst,
    int64_t num_elements)
{
#ifdef __x86_64__
    if (__builtin_cpu_supports("avx2"))
    {
        encode16_AVX2(src, dst, num_elements);
        return;
    }
    if (__builtin_cpu_supports("sse2"))
    {
        encode16_SSE2(src, dst, num_elements);
        return;
    }
#endif
    auto u8 = [](uint8_t v) -> uint64_t { return v; };
    constexpr int S = 8;
    for (int b0 = 0; b0 < 16; b0 += S) {
        int bend = b0 + S <= 16 ? b0 + S : 16;
        int64_t i = 0;
        for (; i + 8 <= num_elements; i += 8) {
            for (int64_t b = b0; b < bend; ++b) {
                uint64_t r = u8(src[(i + 0) * 16 + b])
                           | (u8(src[(i + 1) * 16 + b]) <<  8)
                           | (u8(src[(i + 2) * 16 + b]) << 16)
                           | (u8(src[(i + 3) * 16 + b]) << 24)
                           | (u8(src[(i + 4) * 16 + b]) << 32)
                           | (u8(src[(i + 5) * 16 + b]) << 40)
                           | (u8(src[(i + 6) * 16 + b]) << 48)
                           | (u8(src[(i + 7) * 16 + b]) << 56);
                memcpy(dst + b * num_elements + i, &r, 8);
            }
        }
        for (; i < num_elements; ++i)
            for (int64_t b = b0; b < bend; ++b)
                dst[b * num_elements + i] = src[i * 16 + b];
    }
}

void encodeRuntime(
    const uint8_t * __restrict__ src,
    uint8_t       * __restrict__ dst,
    int64_t num_elements,
    int W)
{
    auto u8 = [](unsigned char v) -> uint64_t { return v; };

    int64_t i = 0;
    for (; i + 16 <= num_elements; i += 16)
    {
        for (int64_t b = 0; b < W; ++b)
        {
            uint64_t lo =
                u8(src[(i +  0) * W + b])        |
               (u8(src[(i +  1) * W + b]) <<  8) |
               (u8(src[(i +  2) * W + b]) << 16) |
               (u8(src[(i +  3) * W + b]) << 24) |
               (u8(src[(i +  4) * W + b]) << 32) |
               (u8(src[(i +  5) * W + b]) << 40) |
               (u8(src[(i +  6) * W + b]) << 48) |
               (u8(src[(i +  7) * W + b]) << 56);
            uint64_t hi =
                u8(src[(i +  8) * W + b])        |
               (u8(src[(i +  9) * W + b]) <<  8) |
               (u8(src[(i + 10) * W + b]) << 16) |
               (u8(src[(i + 11) * W + b]) << 24) |
               (u8(src[(i + 12) * W + b]) << 32) |
               (u8(src[(i + 13) * W + b]) << 40) |
               (u8(src[(i + 14) * W + b]) << 48) |
               (u8(src[(i + 15) * W + b]) << 56);

            memcpy(dst + b * num_elements + i,     &lo, 8);
            memcpy(dst + b * num_elements + i + 8, &hi, 8);
        }
    }
    for (; i < num_elements; ++i)
        for (int64_t b = 0; b < W; ++b)
            dst[b * num_elements + i] = src[i * W + b];
}


// =============================================================================
//  Decode implementations (raw transpose — no header)
// =============================================================================

template <int W>
__attribute__((always_inline)) inline
void decodeW(
    const uint8_t * __restrict__ src,
    uint8_t       * __restrict__ dst,
    int64_t num_elements)
{
    for (int64_t i = 0; i < num_elements; ++i)
        for (int64_t b = 0; b < W; ++b)
            dst[i * W + b] = src[b * num_elements + i];
}

template <>
__attribute__((always_inline)) inline
void decodeW<16>(
    const uint8_t * __restrict__ src,
    uint8_t       * __restrict__ dst,
    int64_t num_elements)
{
#ifdef __x86_64__
    if (__builtin_cpu_supports("avx2"))
    {
        decode16_AVX2(src, dst, num_elements);
        return;
    }
    if (__builtin_cpu_supports("sse2"))
    {
        decode16_SSE2(src, dst, num_elements);
        return;
    }
#endif
    alignas(32) uint8_t tile[16 * 16];
    const int64_t T = num_elements / 16;
    for (int64_t t = 0; t < T; ++t) {
        for (int b = 0; b < 16; ++b)
            for (int i = 0; i < 16; ++i)
                tile[i * 16 + b] = src[static_cast<int64_t>(b) * num_elements + t * 16 + i];
        for (int i = 0; i < 16; ++i)
            memcpy(dst + (t * 16 + i) * 16, &tile[i * 16], 16);
    }
    for (int64_t i = T * 16; i < num_elements; ++i)
        for (int b = 0; b < 16; ++b)
            dst[i * 16 + b] = src[static_cast<int64_t>(b) * num_elements + i];
}

void decodeRuntime(
    const uint8_t * __restrict__ src,
    uint8_t       * __restrict__ dst,
    int64_t num_elements,
    int W)
{
    int64_t i = 0;

    // Process 16 elements at a time: read two uint64_t per stream
    for (; i + 16 <= num_elements; i += 16) {
        // NOTE: stack-allocated, sized by MAX_ELEMENT_WIDTH. Must heap-allocate if MAX_ELEMENT_WIDTH grows large.
        uint64_t s[MAX_ELEMENT_WIDTH][2];
        for (int b = 0; b < W; ++b) {
            memcpy(&s[b][0], src + b * num_elements + i,     8);
            memcpy(&s[b][1], src + b * num_elements + i + 8, 8);
        }

        // Unpack all 16 elements, 4 at a time
        for (int chunk = 0; chunk < 16; chunk += 4) {
            int qword = chunk / 8;       // which uint64_t (0 or 1)
            int shift  = (chunk % 8) * 8; // bit offset within that qword

            uint8_t *e0 = dst + (i + chunk + 0) * W;
            uint8_t *e1 = dst + (i + chunk + 1) * W;
            uint8_t *e2 = dst + (i + chunk + 2) * W;
            uint8_t *e3 = dst + (i + chunk + 3) * W;

            for (int b = 0; b < W; ++b) {
                uint32_t four = static_cast<uint32_t>(s[b][qword] >> shift);
                e0[b] = static_cast<uint8_t>(four);
                e1[b] = static_cast<uint8_t>(four >> 8);
                e2[b] = static_cast<uint8_t>(four >> 16);
                e3[b] = static_cast<uint8_t>(four >> 24);
            }
        }
    }

    // Scalar tail
    for (; i < num_elements; ++i)
        for (int b = 0; b < W; ++b)
            dst[i * W + b] = src[b * num_elements + i];
}


// =============================================================================
//  Top-level raw dispatch (no header — for benchmarks)
// =============================================================================

void encode(const uint8_t * src, uint8_t * dst, int64_t num_elements, int element_bytes)
{
    switch (element_bytes)
    {
        case 2:  encodeW<2> (src, dst, num_elements); return;
        case 4:  encodeW<4> (src, dst, num_elements); return;
        case 8:  encodeW<8> (src, dst, num_elements); return;
        case 16: encodeW<16>(src, dst, num_elements); return;
        default: encodeRuntime(src, dst, num_elements, element_bytes); return;
    }
}

void decode(const uint8_t * src, uint8_t * dst, int64_t num_elements, int element_bytes)
{
    switch (element_bytes)
    {
        case 2:  decodeW<2> (src, dst, num_elements); return;
        case 4:  decodeW<4> (src, dst, num_elements); return;
        case 8:  decodeW<8> (src, dst, num_elements); return;
        case 16: decodeW<16>(src, dst, num_elements); return;
        default: decodeRuntime(src, dst, num_elements, element_bytes); return;
    }
}


// =============================================================================
//  Codec-level compress / decompress (with header — mirrors ClickHouse)
//  These are the functions that the security tests exercise.
//
//  HEADER (5 bytes):
//    [0..3]  int32_t   element_bytes_size  (little-endian via memcpy)
//    [4]     uint8_t   bytes_to_skip       (written for compat; ignored on read)
// =============================================================================

/// Mirrors CompressionCodecByteStreamSplit::doCompressData
uint32_t codec_compress(const uint8_t * source, uint32_t source_size, uint8_t * dest, int32_t element_bytes_size)
{
    uint32_t bytes_to_skip = source_size % element_bytes_size;

    // Store element width as little-endian int32
    int32_t le_width = element_bytes_size;
#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
    le_width = __builtin_bswap32(le_width);
#endif
    memcpy(dest, &le_width, 4);
    dest[4] = static_cast<uint8_t>(bytes_to_skip);

    if (bytes_to_skip)
        memcpy(dest + HEADER_SIZE, source, bytes_to_skip);

    const uint8_t * aligned_source = source + bytes_to_skip;
    uint8_t       * body_dest      = dest   + HEADER_SIZE + bytes_to_skip;
    uint32_t        aligned_size   = source_size - bytes_to_skip;
    int64_t         num_elements   = static_cast<int64_t>(aligned_size / element_bytes_size);

    if (num_elements > 0)
        encode(aligned_source, body_dest, num_elements, element_bytes_size);

    return HEADER_SIZE + source_size;
}

/// Mirrors the FIXED CompressionCodecByteStreamSplit::doDecompressData
/// with all three security fixes applied.
uint32_t codec_decompress(const uint8_t * source, uint32_t source_size, uint8_t * dest, uint32_t uncompressed_size)
{
    if (source_size < HEADER_SIZE)
        throw std::runtime_error("source too small");

    if (uncompressed_size == 0)
        return 0;

    // Read element width as little-endian int32
    int32_t saved_element_bytes;
    memcpy(&saved_element_bytes, source, 4);
#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
    saved_element_bytes = __builtin_bswap32(saved_element_bytes);
#endif

    // FIX 1: reject element size < 2 or > MAX_ELEMENT_WIDTH
    if (saved_element_bytes < 2)
        throw std::runtime_error("invalid element size in header (must be >= 2)");
    if (saved_element_bytes > MAX_ELEMENT_WIDTH)
        throw std::runtime_error("invalid element size in header (exceeds MAX_ELEMENT_WIDTH)");

    // FIX 2: recompute bytes_to_skip from uncompressed_size, don't trust source[4]
    uint32_t bytes_to_skip = uncompressed_size % saved_element_bytes;

    if (source_size < static_cast<uint32_t>(HEADER_SIZE + bytes_to_skip))
        throw std::runtime_error("source too small for header + tail");

    if (bytes_to_skip)
        memcpy(dest, source + HEADER_SIZE, bytes_to_skip);

    uint32_t aligned_uncompressed = uncompressed_size - bytes_to_skip;

    if (aligned_uncompressed == 0)
        return bytes_to_skip;

    // FIX 3: validate source body has enough bytes before decode
    uint32_t body_size = source_size - HEADER_SIZE - bytes_to_skip;
    if (body_size < aligned_uncompressed)
        throw std::runtime_error("source body too small for expected uncompressed size");

    if (aligned_uncompressed % saved_element_bytes != 0)
        throw std::runtime_error("aligned uncompressed size not a multiple of element size");

    int64_t         num_elements = static_cast<int64_t>(aligned_uncompressed / saved_element_bytes);
    const uint8_t * body_src     = source + HEADER_SIZE + bytes_to_skip;
    uint8_t       * aligned_dest = dest   + bytes_to_skip;

    decode(body_src, aligned_dest, num_elements, saved_element_bytes);

    return uncompressed_size;
}


// =============================================================================
//  Correctness checks
// =============================================================================

static void fill_pattern(std::vector<uint8_t> & buf, uint64_t seed)
{
    uint64_t s = seed;
    for (auto & b : buf) {
        s = s * 6364136223846793005ULL + 1442695040888963407ULL;
        b = static_cast<uint8_t>(s >> 56);
    }
}

static bool verify_one(int W, int64_t N, const char * label)
{
    int64_t sz = N * W;
    std::vector<uint8_t> original(sz), encoded(sz, 0xCD), decoded(sz, 0xEF);

    fill_pattern(original, static_cast<uint64_t>(W) * 1000 + N);

    encode(original.data(), encoded.data(), N, W);
    decode(encoded.data(), decoded.data(), N, W);

    if (memcmp(original.data(), decoded.data(), sz) != 0)
    {
        for (int64_t i = 0; i < sz; ++i) {
            if (original[i] != decoded[i]) {
                printf("    FAIL  W=%-3d  N=%-6lld  %-28s  "
                       "first mismatch at byte %lld: orig=%02x decoded=%02x\n",
                       W, (long long)N, label, (long long)i, original[i], decoded[i]);
                return false;
            }
        }
    }
    printf("    OK    W=%-3d  N=%-6lld  %s\n", W, (long long)N, label);
    return true;
}

bool verify_all()
{
    bool all_ok = true;

    printf("Correctness (raw encode/decode round-trip):\n");

    for (int W : {2, 4, 8, 16, 20, 32, 64, 128})
    {
        int64_t simd_block = (W == 16) ? 32 : (W > 16 ? 16 : 8);

        int64_t N_aligned = simd_block * 64;
        all_ok &= verify_one(W, N_aligned,       "aligned (×simd_block)");
        all_ok &= verify_one(W, N_aligned + 1,   "aligned+1 (1-elem tail)");
        all_ok &= verify_one(W, N_aligned - 1,   "aligned-1 (partial block)");
        all_ok &= verify_one(W, simd_block / 2,  "half simd_block");
        all_ok &= verify_one(W, 1,               "N=1 (single element)");
        all_ok &= verify_one(W, 2,               "N=2");
        all_ok &= verify_one(W, 103,             "N=103 (odd prime)");
        all_ok &= verify_one(W, 257,             "N=257 (just past 256)");

        printf("\n");
    }

    return all_ok;
}


// =============================================================================
//  Codec-level round-trip tests (with header)
// =============================================================================

static bool verify_codec_roundtrip_one(int W, int64_t N, const char * label)
{
    int64_t sz = N * W;
    std::vector<uint8_t> original(sz);
    std::vector<uint8_t> compressed(HEADER_SIZE + sz, 0xCD);
    std::vector<uint8_t> decompressed(sz, 0xEF);

    fill_pattern(original, static_cast<uint64_t>(W) * 7777 + N);

    uint32_t comp_size = codec_compress(original.data(), static_cast<uint32_t>(sz),
                                        compressed.data(), static_cast<int32_t>(W));

    if (comp_size != HEADER_SIZE + sz)
    {
        printf("    FAIL  W=%-3d  N=%-6lld  %-35s  compressed size %u != expected %lld\n",
               W, (long long)N, label, comp_size, (long long)(HEADER_SIZE + sz));
        return false;
    }

    uint32_t decomp_size = codec_decompress(compressed.data(), comp_size,
                                            decompressed.data(), static_cast<uint32_t>(sz));

    if (decomp_size != static_cast<uint32_t>(sz))
    {
        printf("    FAIL  W=%-3d  N=%-6lld  %-35s  decompressed size %u != expected %lld\n",
               W, (long long)N, label, decomp_size, (long long)sz);
        return false;
    }

    if (memcmp(original.data(), decompressed.data(), sz) != 0)
    {
        for (int64_t i = 0; i < sz; ++i) {
            if (original[i] != decompressed[i]) {
                printf("    FAIL  W=%-3d  N=%-6lld  %-35s  "
                       "mismatch at byte %lld: orig=%02x got=%02x\n",
                       W, (long long)N, label, (long long)i, original[i], decompressed[i]);
                return false;
            }
        }
    }

    printf("    OK    W=%-3d  N=%-6lld  %s\n", W, (long long)N, label);
    return true;
}

bool verify_codec_roundtrip()
{
    bool all_ok = true;
    printf("Codec round-trip (compress → decompress with header):\n");

    for (int W : {2, 4, 8, 16, 20, 32, 64, 128})
    {
        all_ok &= verify_codec_roundtrip_one(W, 1,    "N=1");
        all_ok &= verify_codec_roundtrip_one(W, 2,    "N=2");
        all_ok &= verify_codec_roundtrip_one(W, 103,  "N=103");
        all_ok &= verify_codec_roundtrip_one(W, 1024, "N=1024");
        printf("\n");
    }

    return all_ok;
}


// =============================================================================
//  Security / robustness validation
//  Each test crafts a malicious compressed buffer and expects decompress
//  to throw rather than read/write out of bounds.
// =============================================================================

static int security_pass = 0;
static int security_fail = 0;

// Helper: expect decompress to throw
static void expect_throw(const char * name, const uint8_t * src, uint32_t src_size,
                         uint8_t * dst, uint32_t uncompressed_size)
{
    try {
        codec_decompress(src, src_size, dst, uncompressed_size);
        printf("    FAIL  %-55s  did NOT throw\n", name);
        security_fail++;
    } catch (const std::runtime_error &) {
        printf("    OK    %-55s  threw as expected\n", name);
        security_pass++;
    }
}

// Helper: expect decompress to succeed
static void expect_ok(const char * name, const uint8_t * src, uint32_t src_size,
                      uint8_t * dst, uint32_t uncompressed_size)
{
    try {
        codec_decompress(src, src_size, dst, uncompressed_size);
        printf("    OK    %-55s  succeeded as expected\n", name);
        security_pass++;
    } catch (const std::runtime_error & e) {
        printf("    FAIL  %-55s  unexpected throw: %s\n", name, e.what());
        security_fail++;
    }
}

// Helper: write an int32 element width into a buffer at offset 0 (little-endian)
static void write_header_width(uint8_t * buf, int32_t width)
{
#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
    width = __builtin_bswap32(width);
#endif
    memcpy(buf, &width, 4);
}

void verify_security()
{
    printf("Security / robustness validation:\n");

    uint8_t dst[4096];
    memset(dst, 0xEE, sizeof(dst));

    // --- 1. Source too small (< HEADER_SIZE) ---
    {
        uint8_t tiny[4] = {4, 0, 0, 0};
        expect_throw("source_size=0", tiny, 0, dst, 100);
        expect_throw("source_size=1", tiny, 1, dst, 100);
        expect_throw("source_size=4 (< 5)", tiny, 4, dst, 100);
    }

    // --- 2. Element size = 0 in header ---
    {
        uint8_t buf[100] = {};
        write_header_width(buf, 0);
        buf[4] = 0;
        expect_throw("element_bytes=0 in header", buf, sizeof(buf), dst, 64);
    }

    // --- 3. Element size = 1 in header (CVE-class: was not rejected) ---
    {
        uint8_t buf[100] = {};
        write_header_width(buf, 1);
        buf[4] = 0;
        expect_throw("element_bytes=1 in header", buf, sizeof(buf), dst, 64);
    }

    // --- 3b. Element size negative in header ---
    {
        uint8_t buf[100] = {};
        write_header_width(buf, -1);
        buf[4] = 0;
        expect_throw("element_bytes=-1 in header", buf, sizeof(buf), dst, 64);
    }

    // --- 3c. Element size exceeds MAX_ELEMENT_WIDTH ---
    {
        uint8_t buf[100] = {};
        write_header_width(buf, 256);
        buf[4] = 0;
        expect_throw("element_bytes=256 in header (> max)", buf, sizeof(buf), dst, 64);
    }

    // --- 3d. Element size = INT32_MAX ---
    {
        uint8_t buf[100] = {};
        write_header_width(buf, INT32_MAX);
        buf[4] = 0;
        expect_throw("element_bytes=INT32_MAX in header", buf, sizeof(buf), dst, 64);
    }

    // --- 4. Valid element size but should work ---
    {
        // Compress 16 bytes with W=4 (4 elements), then decompress
        uint8_t original[16] = {1,2,3,4, 5,6,7,8, 9,10,11,12, 13,14,15,16};
        uint8_t compressed[HEADER_SIZE + 16];
        uint32_t comp_sz = codec_compress(original, 16, compressed, 4);
        expect_ok("valid W=4, 4 elements", compressed, comp_sz, dst, 16);
        if (memcmp(original, dst, 16) == 0)
            printf("           (data matches original)\n");
        else
            printf("           WARNING: data mismatch after valid decompression!\n");
    }

    // --- 5. Truncated source body (heap over-read without fix) ---
    {
        // Legitimate compressed data for W=4, 100 elements = 400 bytes data
        uint32_t data_size = 400;
        std::vector<uint8_t> original(data_size, 0xAB);
        std::vector<uint8_t> compressed(HEADER_SIZE + data_size);
        uint32_t comp_sz = codec_compress(original.data(), data_size, compressed.data(), 4);

        // Now truncate: give only half the body
        uint32_t truncated_sz = HEADER_SIZE + data_size / 2;
        expect_throw("truncated body (half)", compressed.data(), truncated_sz, dst, data_size);

        // Extreme: only the header
        expect_throw("truncated body (header only)", compressed.data(), HEADER_SIZE, dst, data_size);

        // Off by one: body is 1 byte short
        expect_throw("truncated body (off-by-one)", compressed.data(), comp_sz - 1, dst, data_size);
    }

    // --- 6. bytes_to_skip forgery (attacker writes bogus source[4]) ---
    //     With our fix, source[4] is ignored, so this should still work
    //     correctly as long as the body is valid.
    {
        uint8_t original[16] = {1,2,3,4, 5,6,7,8, 9,10,11,12, 13,14,15,16};
        uint8_t compressed[HEADER_SIZE + 16];
        codec_compress(original, 16, compressed, 4);

        // Corrupt source[4] to a bogus value
        uint8_t tampered[HEADER_SIZE + 16];
        memcpy(tampered, compressed, HEADER_SIZE + 16);
        tampered[4] = 3;  // was 0, set to 3 (bogus)

        // Should still decompress correctly because we recompute bytes_to_skip
        // 16 % 4 = 0, so bytes_to_skip = 0 regardless of source[4]
        uint8_t result[16];
        expect_ok("forged bytes_to_skip=3, actual=0", tampered, HEADER_SIZE + 16, result, 16);
        if (memcmp(original, result, 16) == 0)
            printf("           (data matches — source[4] correctly ignored)\n");
        else
            printf("           WARNING: data mismatch — source[4] may still be trusted!\n");
    }

    // --- 7. bytes_to_skip forgery with unaligned data ---
    //     13 bytes with W=4: real bytes_to_skip = 13%4 = 1
    {
        uint8_t original[13];
        for (int i = 0; i < 13; i++) original[i] = static_cast<uint8_t>(i + 1);

        uint8_t compressed[HEADER_SIZE + 13];
        codec_compress(original, 13, compressed, 4);

        // Verify original round-trip works
        uint8_t result[13];
        expect_ok("unaligned W=4, 13 bytes (legit)", compressed, HEADER_SIZE + 13, result, 13);
        if (memcmp(original, result, 13) == 0)
            printf("           (data matches)\n");

        // Now forge source[4] = 0 (attacker claims no remainder)
        uint8_t tampered[HEADER_SIZE + 13];
        memcpy(tampered, compressed, HEADER_SIZE + 13);
        tampered[4] = 0;  // forge: claim no skip bytes

        // Should still decompress correctly: we recompute 13%4 = 1
        memset(result, 0xEE, 13);
        expect_ok("forged bytes_to_skip=0, actual=1", tampered, HEADER_SIZE + 13, result, 13);
        if (memcmp(original, result, 13) == 0)
            printf("           (data matches — source[4] correctly ignored)\n");
        else
            printf("           WARNING: data mismatch — source[4] may still be trusted!\n");
    }

    // --- 8. Element size forgery: header says W=2 but data was compressed with W=4 ---
    {
        uint8_t original[16] = {1,2,3,4, 5,6,7,8, 9,10,11,12, 13,14,15,16};
        uint8_t compressed[HEADER_SIZE + 16];
        codec_compress(original, 16, compressed, 4);

        // Corrupt element size to 2
        int32_t forged_w = 2;
        memcpy(compressed, &forged_w, 4);

        // Should decompress without crashing (body size check passes:
        // 16 bytes body >= 16 bytes expected). It'll produce wrong data
        // but shouldn't crash or read OOB.
        uint8_t result[16];
        expect_ok("forged element_bytes=2 (was 4), same body size", compressed,
                  HEADER_SIZE + 16, result, 16);
        printf("           (data will be wrong but no crash — this is expected)\n");
    }

    // --- 9. Element size forgery causing body size mismatch ---
    {
        // Compress 8 bytes with W=2 (4 elements)
        uint8_t original[8] = {1,2,3,4,5,6,7,8};
        uint8_t compressed[HEADER_SIZE + 8];
        codec_compress(original, 8, compressed, 2);

        // Forge element size to 4, claim uncompressed_size = 400
        // body is only 8 bytes, but 400 bytes expected → should throw
        int32_t forged_w = 4;
        memcpy(compressed, &forged_w, 4);
        expect_throw("forged element_bytes=4, inflated uncompressed_size",
                     compressed, HEADER_SIZE + 8, dst, 400);
    }

    // --- 10. uncompressed_size = 0 (edge case) ---
    {
        uint8_t buf[10] = {};
        write_header_width(buf, 4);
        buf[4] = 0;
        expect_ok("uncompressed_size=0", buf, 10, dst, 0);
    }

    // --- 11. Aligned uncompressed not a multiple of element size ---
    //     This check is now effectively dead code with recomputed bytes_to_skip,
    //     because aligned = uncompressed - (uncompressed % W) is always divisible by W.
    //     Just verify it doesn't fire for valid data.
    {
        uint8_t original[12] = {1,2,3,4,5,6,7,8,9,10,11,12};
        uint8_t compressed[HEADER_SIZE + 12];
        codec_compress(original, 12, compressed, 4);
        uint8_t result[12];
        expect_ok("modulo check (defense-in-depth, W=4, 12 bytes)",
                  compressed, HEADER_SIZE + 12, result, 12);
    }

    // --- 12. Large element size (max = 255) ---
    {
        // Valid: W=255, 1 element = 255 bytes
        uint32_t sz = 255;
        std::vector<uint8_t> original(sz, 0x42);
        std::vector<uint8_t> compressed(HEADER_SIZE + sz);
        codec_compress(original.data(), sz, compressed.data(), 255);
        std::vector<uint8_t> result(sz, 0);
        expect_ok("W=255, 1 element (max width)", compressed.data(),
                  HEADER_SIZE + sz, result.data(), sz);
        if (memcmp(original.data(), result.data(), sz) == 0)
            printf("           (data matches)\n");
    }

    printf("\n  Security results: %d passed, %d failed\n", security_pass, security_fail);
}


// =============================================================================
//  Benchmark harness
// =============================================================================

struct Stats { double min_gbps, med_gbps; };

Stats bench(int64_t data_size, int W, bool do_encode, int rounds, int inner)
{
    int64_t N = data_size / W;
    int64_t sz = N * W;
    std::vector<uint8_t> src(sz, 0xAB), dst(sz, 0);

    if (!do_encode)
        encode(src.data(), dst.data(), N, W);

    const uint8_t * in  = do_encode ? src.data() : dst.data();
    uint8_t       * out = do_encode ? dst.data() : src.data();

    for (int i = 0; i < 3; ++i)
    {
        if (do_encode) encode(in, out, N, W);
        else           decode(in, out, N, W);
    }

    std::vector<double> results;
    results.reserve(rounds);

    for (int r = 0; r < rounds; ++r)
    {
        auto t0 = Clock::now();
        for (int i = 0; i < inner; ++i)
        {
            if (do_encode) encode(in, out, N, W);
            else           decode(in, out, N, W);
        }
        auto t1 = Clock::now();
        double sec = std::chrono::duration<double>(t1 - t0).count() / inner;
        results.push_back((sz / 1e9) / sec);
    }
    std::sort(results.begin(), results.end());
    return { results.front(), results[results.size() / 2] };
}

void print_row(const char * label, Stats s)
{
    printf("    %-12s  min=%6.1f  med=%6.1f GB/s\n", label, s.min_gbps, s.med_gbps);
}

int main()
{
    // --- Phase 1: raw encode/decode round-trip ---
    if (!verify_all())
    {
        printf("Raw correctness failures — aborting.\n");
        return 1;
    }
    printf("\n");

    // --- Phase 2: codec-level round-trip (with header) ---
    if (!verify_codec_roundtrip())
    {
        printf("Codec round-trip failures — aborting.\n");
        return 1;
    }
    printf("\n");

    // --- Phase 3: security / robustness validation ---
    verify_security();
    if (security_fail > 0)
    {
        printf("\nSecurity failures detected — aborting benchmark.\n");
        return 1;
    }
    printf("\n");

    // --- Phase 4: benchmark ---
    const int64_t DATA_SIZE = 250LL << 20;
    const int rounds = 10, inner = 4;

    for (int W : {2, 4, 8, 16, 20, 32, 64, 128})
    {
        const char * path;
        if (W == 16)       path = "encodeW<16>/decodeW<16> (SIMD)";
        else if (W <= 8)   path = "encodeW<W>/decodeW<W> (scalar unroll)";
        else               path = "encodeRuntime/decodeRuntime";

        printf("=== W=%d (%s) ===\n", W, path);

        printf("  ENCODE:\n");
        print_row("throughput", bench(DATA_SIZE, W, true, rounds, inner));

        printf("  DECODE:\n");
        print_row("throughput", bench(DATA_SIZE, W, false, rounds, inner));

        printf("\n");
    }

    return 0;
}