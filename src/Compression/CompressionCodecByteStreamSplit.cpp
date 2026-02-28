#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <Common/TargetSpecific.h>
#include <DataTypes/IDataType.h>
#include <base/unaligned.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <IO/WriteHelpers.h>

#include <vector>
#include <bit>

#ifdef __x86_64__
#include <immintrin.h>
#endif


// =============================================================================
//  ByteStreamSplit codec
//
//  A preprocessing transform that improves compression of fixed-width
//  columnar data. Given N elements of W bytes each, it transposes the
//  byte layout so that all first bytes are contiguous, then all second
//  bytes, and so on — producing W streams of N bytes each.
//
//  Example with W=4 (Float32), 3 elements:
//
//    Input  (row-major):  [A0 A1 A2 A3] [B0 B1 B2 B3] [C0 C1 C2 C3]
//    Output (transposed): [A0 B0 C0] [A1 B1 C1] [A2 B2 C2] [A3 B3 C3]
//
//  Bytes at the same position within each element tend to have similar
//  values (e.g. exponent bytes of floats are nearly identical across
//  rows), so grouping them together creates long runs of similar bytes
//  that compress dramatically better with a subsequent codec like LZ4
//  or ZSTD.
//
//  This is the same transform used by Apache Parquet/Arrow's
//  BYTE_STREAM_SPLIT encoding and is especially effective for:
//    - Floating point: Float32, Float64
//    - Fixed-width types: UUID, IPv6, Decimal128, FixedString
//    - Any type where adjacent elements share byte-level structure
// =============================================================================
// =============================================================================
//  Throughput reference (GB/s)
//  Hardware: Intel i7-12500H (12th gen Alder Lake)
//  Data: 500 MiB, 40 rounds × 8 inner iterations, averaged over 2 runs
//
//                    ENCODE            DECODE
//  W     path         min   median      min   median
//  ---   ----------  -----  -----      -----  -----
//   2    encodeW      10.6   11.1       10.7   11.1
//   4    encodeW      10.4   10.6       11.0   11.3
//   8    encodeW       8.9   9.3        10.2   10.6
//  16    AVX2 SIMD     6.7   6.7         9.3    9.5
//  20    runtime       4.6   4.9         4.3    4.4
//  32    runtime       4.7   4.8         4.3    4.4
//  64    runtime       4.7   4.8         4.5    4.5
// 128    runtime       4.2   4.6         4.3    4.4
// =============================================================================
namespace DB
{


class CompressionCodecByteStreamSplit : public ICompressionCodec
{
public:
    static constexpr UInt32 HEADER_SIZE = 2;

    explicit CompressionCodecByteStreamSplit(UInt8 element_bytes_size_);

    uint8_t getMethodByte() const override;
    void updateHash(SipHash & hash) const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
    UInt32 doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override
    {
        return uncompressed_size + HEADER_SIZE;
    }

    bool isCompression() const override { return false; }
    bool isGenericCompression() const override { return false; }
    bool isExperimental() const override { return true; }

    String getDescription() const override
    {
        return "Preprocessor (should be followed by a compression codec). "
               "Transposes bytes of fixed-width elements so that bytes at the "
               "same position within each element are grouped into contiguous "
               "streams. Significantly improves compression of floating-point "
               "and other fixed-width columnar data (Float32, Float64, "
               "Decimal128, UUID, IPv6, FixedString).";
    }

private:
    const UInt8 element_bytes_size;
};


namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
    extern const int ILLEGAL_CODEC_PARAMETER;
    extern const int BAD_ARGUMENTS;
}


namespace
{

// Maximum element width in bytes. Enforced at codec registration.
// Fits in UInt8, so 255 is the hard ceiling.
static constexpr int MAX_ELEMENT_WIDTH = 255;

// =============================================================================
//  W=16 SIMD intrinsics — SSE2 butterfly transpose (encode + decode)
// =============================================================================

#ifdef __x86_64__

/// Performs a 16×16 byte matrix transpose using the SSE2 unpack butterfly.
///
/// Each input register R[i] holds 16 bytes representing one element (row).
/// After the butterfly, V[b] holds byte-position b from all 16 elements —
/// i.e. the rows and columns are swapped.
///
/// The butterfly works in 3 stages of unpack operations:
///   stage 1: unpacklo/hi epi8  → interleave byte pairs
///   stage 2: unpacklo/hi epi16 → interleave 16-bit groups
///   stage 3: unpacklo/hi epi32 → interleave 32-bit groups
/// A final epi64 unpack in storeRows/storeBands completes the last stage.
__attribute__((target("sse2")))
static inline void butterfly16(const __m128i R[16], __m128i V[16])
{
    // Stage 1: interleave adjacent rows at byte granularity
    __m128i T[16];
    for (int k = 0; k < 8; ++k)
    {
        T[2 * k]     = _mm_unpacklo_epi8(R[2 * k], R[2 * k + 1]);
        T[2 * k + 1] = _mm_unpackhi_epi8(R[2 * k], R[2 * k + 1]);
    }

    // Stage 2: interleave at 16-bit granularity
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

    // Stage 3: interleave at 32-bit granularity
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
    // Final epi64 stage is deferred to the store helpers below
}

/// Write butterfly output as 16 contiguous rows of 16 bytes each (decode path).
/// Completes the final epi64 butterfly stage inline during store.
__attribute__((target("sse2")))
static inline void storeRows(const __m128i V[16], uint8_t * base)
{
    for (int k = 0; k < 8; ++k)
    {
        _mm_storeu_si128(reinterpret_cast<__m128i *>(base + (2 * k)     * 16), _mm_unpacklo_epi64(V[k], V[k + 8]));
        _mm_storeu_si128(reinterpret_cast<__m128i *>(base + (2 * k + 1) * 16), _mm_unpackhi_epi64(V[k], V[k + 8]));
    }
}

/// Write butterfly output into 16 separate byte-stream band pointers (encode path).
/// Each band w[b] receives the bytes from position b across all elements.
/// Completes the final epi64 butterfly stage inline during store.
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
//  AVX2 encode for W=16: 4-stage deinterleave, processes 32 rows at a time
// =============================================================================

/// Split one 256-bit register pair into even-indexed and odd-indexed bytes.
/// Used as one stage of the W=16 encode: separates byte positions 0,2,4,...
/// from 1,3,5,... across two input vectors, then packs and lane-fixes them.
/// Similar role to one level of the SSE2 butterfly, but operates on 32 rows.
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
    // pack pairs back to bytes, then fix AVX2 lane ordering
    even_out = _mm256_permute4x64_epi64(_mm256_packus_epi16(a_even, b_even), 0xD8);
    odd_out  = _mm256_permute4x64_epi64(_mm256_packus_epi16(a_odd,  b_odd),  0xD8);
}

/// AVX2 encode for W=16. Processes 32 elements per iteration using 4 stages
/// of splitEvenOddBytes to fully separate all 16 byte positions.
/// Falls back to scalar loop for the remaining < 32 elements.
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

        // Load 32 elements × 16 bytes = 16 AVX2 registers of 32 bytes each
        __m256i r[16];
        for (int k = 0; k < 16; ++k)
            r[k] = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(s + k * 32));

        // Stage 1: separate byte positions 0,2,4,... from 1,3,5,...
        __m256i s1[16];
        for (int k = 0; k < 8; ++k)
            splitEvenOddBytes(r[2 * k], r[2 * k + 1], s1[k], s1[k + 8], mask);

        // Stage 2: further separate into groups of 4
        __m256i s2[16];
        for (int k = 0; k < 4; ++k)
            splitEvenOddBytes(s1[2 * k], s1[2 * k + 1], s2[k], s2[k + 4], mask);
        for (int k = 0; k < 4; ++k)
            splitEvenOddBytes(s1[8 + 2 * k], s1[8 + 2 * k + 1], s2[8 + k], s2[8 + k + 4], mask);

        // Stage 3: further separate into groups of 2
        __m256i s3[16];
        for (int k = 0; k < 2; ++k)
            splitEvenOddBytes(s2[2 * k], s2[2 * k + 1], s3[k], s3[k + 2], mask);
        for (int k = 0; k < 2; ++k)
            splitEvenOddBytes(s2[4 + 2 * k], s2[4 + 2 * k + 1], s3[4 + k], s3[4 + k + 2], mask);
        for (int k = 0; k < 2; ++k)
            splitEvenOddBytes(s2[8 + 2 * k], s2[8 + 2 * k + 1], s3[8 + k], s3[8 + k + 2], mask);
        for (int k = 0; k < 2; ++k)
            splitEvenOddBytes(s2[12 + 2 * k], s2[12 + 2 * k + 1], s3[12 + k], s3[12 + k + 2], mask);

        // Stage 4: final separation — each out[k] is one complete byte-stream
        __m256i out[16];
        for (int g = 0; g < 8; ++g)
            splitEvenOddBytes(s3[2 * g], s3[2 * g + 1], out[g], out[g + 8], mask);

        // The 4-stage butterfly produces streams in a bit-reversal permuted order.
        // band_of[k] maps output register k to its correct byte-stream index.
        static const int band_of[16] = {0, 4, 2, 6, 1, 5, 3, 7, 8, 12, 10, 14, 9, 13, 11, 15};
        for (int k = 0; k < 16; ++k)
            _mm256_storeu_si256(reinterpret_cast<__m256i *>(dst + static_cast<int64_t>(band_of[k]) * N + i), out[k]);
    }
    _mm256_zeroupper();

    // Scalar tail: same as encodeW<16> scalar path
    for (; i < N; ++i)
        for (int b = 0; b < 16; ++b)
            dst[static_cast<int64_t>(b) * N + i] = src[i * 16 + b];
}

// =============================================================================
//  SSE2 encode for W=16: butterfly transpose, 16 rows at a time
// =============================================================================

/// SSE2 encode for W=16. Processes 16 elements per iteration via butterfly16.
/// Falls back to scalar loop for the remaining < 16 elements.
__attribute__((target("sse2")))
static void encode16_SSE2(
    const uint8_t * __restrict__ src,
    uint8_t       * __restrict__ dst,
    int64_t N)
{
    int64_t i = 0;
    for (; i + 16 <= N; i += 16)
    {
        // Load 16 elements, one per register
        __m128i R[16], V[16];
        for (int k = 0; k < 16; ++k)
            R[k] = _mm_loadu_si128(reinterpret_cast<const __m128i *>(src + (i + k) * 16));
        butterfly16(R, V);

        // Write each transposed column into its byte-stream band
        uint8_t * w[16];
        for (int b = 0; b < 16; ++b)
            w[b] = dst + static_cast<int64_t>(b) * N;
        storeBands(V, w, i);
    }
    // Scalar tail: same as encodeW<16> scalar path
    for (; i < N; ++i)
        for (int b = 0; b < 16; ++b)
            dst[static_cast<int64_t>(b) * N + i] = src[i * 16 + b];
}

// =============================================================================
//  AVX2 decode for W=16: load 256-bit bands, split to 2×butterfly16
// =============================================================================

/// AVX2 decode for W=16. Loads 32 elements worth of band data per iteration,
/// splits into two 16-element halves, and runs butterfly16 on each half.
/// Falls back to scalar loop for the remaining < 32 elements.
__attribute__((target("avx2")))
static void decode16_AVX2(
    const uint8_t * __restrict__ src,
    uint8_t       * __restrict__ dst,
    int64_t N)
{
    // One pointer per byte-stream band
    const uint8_t * r[16];
    for (int b = 0; b < 16; ++b)
        r[b] = src + static_cast<int64_t>(b) * N;

    int64_t i = 0;
    for (; i + 32 <= N; i += 32)
    {
        // Load 32 bytes from each band, split into low/high 16-byte halves
        __m128i Lo[16], Hi[16], VL[16], VH[16];
        for (int b = 0; b < 16; ++b)
        {
            __m256i tmp = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(r[b] + i));
            Lo[b] = _mm256_castsi256_si128(tmp);
            Hi[b] = _mm256_extracti128_si256(tmp, 1);
        }
        // Butterfly each half independently, write 16 restored elements each
        butterfly16(Lo, VL);
        storeRows(VL, dst + i * 16);
        butterfly16(Hi, VH);
        storeRows(VH, dst + (i + 16) * 16);
    }
    _mm256_zeroupper();

    // Scalar tail: same as decodeW<16> scalar path
    for (; i < N; ++i)
        for (int b = 0; b < 16; ++b)
            dst[i * 16 + b] = r[b][i];
}

// =============================================================================
//  SSE2 decode for W=16: butterfly transpose, 16 rows at a time
// =============================================================================

/// SSE2 decode for W=16. Processes 16 elements per iteration via butterfly16.
/// Falls back to scalar loop for the remaining < 16 elements.
__attribute__((target("sse2")))
static void decode16_SSE2(
    const uint8_t * __restrict__ src,
    uint8_t       * __restrict__ dst,
    int64_t N)
{
    // One pointer per byte-stream band
    const uint8_t * r[16];
    for (int b = 0; b < 16; ++b)
        r[b] = src + static_cast<int64_t>(b) * N;

    int64_t i = 0;
    for (; i + 16 <= N; i += 16)
    {
        // Load 16 bytes from each band (one element per band position)
        __m128i R[16], V[16];
        for (int b = 0; b < 16; ++b)
            R[b] = _mm_loadu_si128(reinterpret_cast<const __m128i *>(r[b] + i));
        butterfly16(R, V);
        storeRows(V, dst + i * 16);
    }
    // Scalar tail: same as decodeW<16> scalar path
    for (; i < N; ++i)
        for (int b = 0; b < 16; ++b)
            dst[i * 16 + b] = r[b][i];
}

#endif // __x86_64__

// =============================================================================
//  Encode helpers
// =============================================================================

/// Scalar encode for compile-time-known element width W.
/// Scatters byte b of element i into stream b at offset i.
/// The compiler fully unrolls the inner loop since W is a compile-time constant.
template <int W>
ALWAYS_INLINE void encodeW(
    const char * __restrict__ src,
    char       * __restrict__ dst,
    int64_t num_elements)
{
    for (int64_t i = 0; i < num_elements; ++i)
        for (int64_t b = 0; b < W; ++b)
            dst[b * num_elements + i] = src[i * W + b];
}

/// W=16 specialisation: dispatches to AVX2 > SSE2 > scalar fallback.
/// The scalar fallback packs 8 scattered bytes into a uint64_t per write,
/// same idea as encodeRuntime but unrolled for W=16.
template <>
ALWAYS_INLINE void encodeW<16>(
    const char * __restrict__ src,
    char       * __restrict__ dst,
    int64_t num_elements)
{
    const auto * __restrict__ s = reinterpret_cast<const uint8_t *>(src);
    auto       * __restrict__ d = reinterpret_cast<uint8_t *>(dst);

#ifdef __x86_64__
    if (__builtin_cpu_supports("avx2"))
    {
        encode16_AVX2(s, d, num_elements);
        return;
    }
    if (__builtin_cpu_supports("sse2"))
    {
        encode16_SSE2(s, d, num_elements);
        return;
    }
#endif

    // Scalar fallback: packs 8 elements at a time into uint64_t to reduce
    // store count — same strategy as encodeRuntime, manually unrolled for W=16.
    if constexpr (std::endian::native == std::endian::little)
    {
        auto u8 = [](uint8_t v) -> uint64_t { return v; };
        constexpr int S = 8;
        for (int b0 = 0; b0 < 16; b0 += S) {
            int bend = b0 + S <= 16 ? b0 + S : 16;
            int64_t i = 0;
            for (; i + 8 <= num_elements; i += 8) {
                for (int64_t b = b0; b < bend; ++b) {
                    uint64_t r = u8(s[(i + 0) * 16 + b])
                               | (u8(s[(i + 1) * 16 + b]) <<  8)
                               | (u8(s[(i + 2) * 16 + b]) << 16)
                               | (u8(s[(i + 3) * 16 + b]) << 24)
                               | (u8(s[(i + 4) * 16 + b]) << 32)
                               | (u8(s[(i + 5) * 16 + b]) << 40)
                               | (u8(s[(i + 6) * 16 + b]) << 48)
                               | (u8(s[(i + 7) * 16 + b]) << 56);
                    memcpy(d + b * num_elements + i, &r, 8);
                }
            }
            for (; i < num_elements; ++i)
                for (int64_t b = b0; b < bend; ++b)
                    d[b * num_elements + i] = s[i * 16 + b];
        }
    }
    else
    {
        // Big-endian: uint64_t packing would reverse byte order, use plain scalar
        for (int64_t i = 0; i < num_elements; ++i)
            for (int64_t b = 0; b < 16; ++b)
                d[b * num_elements + i] = s[i * 16 + b];
    }
}

/// Runtime-W encode for element widths not handled by encodeW<W> specialisations
/// (i.e. W != 2, 4, 8, 16). Processes 16 elements per outer iteration, packing
/// each byte stream into two uint64_t writes to reduce scatter store overhead.
/// Falls back to plain scalar loop for the remaining < 8 elements.
ALWAYS_INLINE void encodeRuntime(
    const char * __restrict__ src,
    char       * __restrict__ dst,
    int64_t num_elements,
    UInt8  W)
{
    const auto * __restrict__ s = reinterpret_cast<const unsigned char *>(src);
    auto       * __restrict__ d = reinterpret_cast<unsigned char *>(dst);

    if constexpr (std::endian::native == std::endian::little)
    {
        auto u8 = [](unsigned char v) -> uint64_t { return v; };

        int64_t i = 0;
        // Main loop: 16 elements at a time, two 8-element uint64_t packs per stream
        for (; i + 16 <= num_elements; i += 16)
        {
            for (int64_t b = 0; b < W; ++b)
            {
                uint64_t lo =
                    u8(s[(i +  0) * W + b])        |
                   (u8(s[(i +  1) * W + b]) <<  8) |
                   (u8(s[(i +  2) * W + b]) << 16) |
                   (u8(s[(i +  3) * W + b]) << 24) |
                   (u8(s[(i +  4) * W + b]) << 32) |
                   (u8(s[(i +  5) * W + b]) << 40) |
                   (u8(s[(i +  6) * W + b]) << 48) |
                   (u8(s[(i +  7) * W + b]) << 56);
                uint64_t hi =
                    u8(s[(i +  8) * W + b])        |
                   (u8(s[(i +  9) * W + b]) <<  8) |
                   (u8(s[(i + 10) * W + b]) << 16) |
                   (u8(s[(i + 11) * W + b]) << 24) |
                   (u8(s[(i + 12) * W + b]) << 32) |
                   (u8(s[(i + 13) * W + b]) << 40) |
                   (u8(s[(i + 14) * W + b]) << 48) |
                   (u8(s[(i + 15) * W + b]) << 56);

                memcpy(d + b * num_elements + i,     &lo, 8);
                memcpy(d + b * num_elements + i + 8, &hi, 8);
            }
        }
        // Middle chunk: 8 elements at a time, one uint64_t pack per stream
        for (; i + 8 <= num_elements; i += 8)
        {
            for (int64_t b = 0; b < W; ++b)
            {
                uint64_t r =
                    u8(s[(i + 0) * W + b])        |
                   (u8(s[(i + 1) * W + b]) <<  8) |
                   (u8(s[(i + 2) * W + b]) << 16) |
                   (u8(s[(i + 3) * W + b]) << 24) |
                   (u8(s[(i + 4) * W + b]) << 32) |
                   (u8(s[(i + 5) * W + b]) << 40) |
                   (u8(s[(i + 6) * W + b]) << 48) |
                   (u8(s[(i + 7) * W + b]) << 56);

                memcpy(d + b * num_elements + i, &r, 8);
            }
        }
        // Scalar tail: plain scatter, same as encodeW<W> inner loop
        for (; i < num_elements; ++i)
            for (int64_t b = 0; b < W; ++b)
                d[b * num_elements + i] = s[i * W + b];
    }
    else
    {
        // Big-endian: uint64_t packing would reverse byte order, use plain scalar
        for (int64_t i = 0; i < num_elements; ++i)
            for (int64_t b = 0; b < W; ++b)
                d[b * num_elements + i] = s[i * W + b];
    }
}

// =============================================================================
//  Decode helpers
// =============================================================================

/// Scalar decode for compile-time-known element width W.
/// Gathers byte b of element i from stream b at offset i.
/// The compiler fully unrolls the inner loop since W is a compile-time constant.
template <int W>
ALWAYS_INLINE void decodeW(
    const char * __restrict__ src,
    char       * __restrict__ dst,
    int64_t num_elements)
{
    for (int64_t i = 0; i < num_elements; ++i)
        for (int64_t b = 0; b < W; ++b)
            dst[i * W + b] = src[b * num_elements + i];
}

/// W=16 specialisation: dispatches to AVX2 > SSE2 > scalar fallback.
/// The scalar fallback uses a 16×16 tile buffer to improve cache behaviour,
/// similar to a cache-oblivious transpose — reads 16 bytes per band
/// contiguously, transposes into the tile, then writes 16 contiguous rows.
template <>
ALWAYS_INLINE void decodeW<16>(
    const char * __restrict__ src,
    char       * __restrict__ dst,
    int64_t num_elements)
{
    const auto * __restrict__ s = reinterpret_cast<const uint8_t *>(src);
    auto       * __restrict__ d = reinterpret_cast<uint8_t *>(dst);

#ifdef __x86_64__
    if (__builtin_cpu_supports("avx2"))
    {
        decode16_AVX2(s, d, num_elements);
        return;
    }
    if (__builtin_cpu_supports("sse2"))
    {
        decode16_SSE2(s, d, num_elements);
        return;
    }
#endif

    // Scalar fallback: tiled 16×16 gather into a local buffer,
    // then write 16 contiguous output rows per tile.
    alignas(32) uint8_t tile[16 * 16];
    const int64_t T = num_elements / 16;
    for (int64_t t = 0; t < T; ++t)
    {
        for (int b = 0; b < 16; ++b)
            for (int i = 0; i < 16; ++i)
                tile[i * 16 + b] = s[static_cast<int64_t>(b) * num_elements + t * 16 + i];
        for (int i = 0; i < 16; ++i)
            memcpy(d + (t * 16 + i) * 16, &tile[i * 16], 16);
    }

    // Scalar tail: plain gather for remaining elements (same as decodeW<W> loop)
    for (int64_t i = T * 16; i < num_elements; ++i)
        for (int b = 0; b < 16; ++b)
            d[i * 16 + b] = s[static_cast<int64_t>(b) * num_elements + i];
}

/// Runtime-W decode for element widths not handled by decodeW<W> specialisations.
/// Processes 8 elements per outer iteration: loads 8 bytes from each of the W
/// byte-stream bands into a local array, then unpacks all 8 elements from it.
/// Falls back to plain scalar loop for the remaining < 8 elements.
ALWAYS_INLINE void decodeRuntime(
    const char * __restrict__ src,
    char       * __restrict__ dst,
    int64_t num_elements,
    UInt8  W)
{
    const auto * __restrict__ s = reinterpret_cast<const unsigned char *>(src);
    auto       * __restrict__ d = reinterpret_cast<unsigned char *>(dst);

    if constexpr (std::endian::native == std::endian::little)
    {
        int64_t i = 0;
        // Main loop: gather 8 bytes from each of the W bands into a uint64_t,
        // then unpack byte-by-byte into 8 consecutive output elements.
        for (; i + 8 <= num_elements; i += 8)
        {
            // One uint64_t per byte stream holds 8 gathered bytes
            uint64_t streams[MAX_ELEMENT_WIDTH];
            for (int64_t b = 0; b < W; ++b)
                memcpy(&streams[b], s + b * num_elements + i, 8);
            for (int ei = 0; ei < 8; ++ei)
            {
                unsigned char * elem = d + (i + ei) * W;
                for (int64_t b = 0; b < W; ++b)
                    elem[b] = static_cast<unsigned char>(streams[b] >> (ei * 8));
            }
        }
        // Scalar tail: plain gather, same as decodeW<W> inner loop
        for (; i < num_elements; ++i)
            for (int64_t b = 0; b < W; ++b)
                d[i * W + b] = s[b * num_elements + i];
    }
    else
    {
        // Big-endian: uint64_t shift extraction would reverse byte order, use plain scalar
        for (int64_t i = 0; i < num_elements; ++i)
            for (int64_t b = 0; b < W; ++b)
                d[i * W + b] = s[b * num_elements + i];
    }
}

// =============================================================================
//  Top-level encode / decode dispatchers
//
//  Dispatch strategy:
//    W == 2, 4, 8  → encodeW<W>/decodeW<W>: compile-time unrolled scalar loop
//    W == 16       → encodeW<16>/decodeW<16>: AVX2 > SSE2 > tiled scalar
//    W == other    → encodeRuntime/decodeRuntime: runtime-W uint64_t batching
// =============================================================================

/// Selects the encode path for the given element width.
/// Called once per compressed block; actual work is in the helpers above.
MULTITARGET_FUNCTION_AVX512BW_AVX512F_AVX2_SSE42(
MULTITARGET_FUNCTION_HEADER(
void), encodeDispatch, MULTITARGET_FUNCTION_BODY((
    const char * __restrict__ src,
    char       * __restrict__ dst,
    int64_t num_elements,
    UInt8  element_bytes) /// NOLINT
{
    switch (element_bytes)
    {
        case 2:  encodeW<2> (src, dst, num_elements); return;
        case 4:  encodeW<4> (src, dst, num_elements); return;
        case 8:  encodeW<8> (src, dst, num_elements); return;
        case 16: encodeW<16>(src, dst, num_elements); return;
        default: encodeRuntime(src, dst, num_elements, element_bytes); return;
    }
})
)

/// Top-level encode entry point. Picks the best ISA variant via
/// USE_MULTITARGET_CODE dispatch, then falls through to encodeDispatch.
ALWAYS_INLINE void encode(
    const char * src,
    char       * dst,
    int64_t num_elements,
    UInt8  element_bytes)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::AVX512BW))
    {
        encodeDispatchAVX512BW(src, dst, num_elements, element_bytes);
        return;
    }
    if (isArchSupported(TargetArch::AVX512F))
    {
        encodeDispatchAVX512F(src, dst, num_elements, element_bytes);
        return;
    }
    if (isArchSupported(TargetArch::AVX2))
    {
        encodeDispatchAVX2(src, dst, num_elements, element_bytes);
        return;
    }
    if (isArchSupported(TargetArch::SSE42))
    {
        encodeDispatchSSE42(src, dst, num_elements, element_bytes);
        return;
    }
#endif
    encodeDispatch(src, dst, num_elements, element_bytes);
}

/// Selects the decode path for the given element width.
/// Called once per decompressed block; actual work is in the helpers above.
MULTITARGET_FUNCTION_AVX512BW_AVX512F_AVX2_SSE42(
MULTITARGET_FUNCTION_HEADER(
void), decodeDispatch, MULTITARGET_FUNCTION_BODY((
    const char * __restrict__ src,
    char       * __restrict__ dst,
    int64_t num_elements,
    UInt8  element_bytes) /// NOLINT
{
    switch (element_bytes)
    {
        case 2:  decodeW<2> (src, dst, num_elements); return;
        case 4:  decodeW<4> (src, dst, num_elements); return;
        case 8:  decodeW<8> (src, dst, num_elements); return;
        case 16: decodeW<16>(src, dst, num_elements); return;
        default: decodeRuntime(src, dst, num_elements, element_bytes); return;
    }
})
)

/// Top-level decode entry point. Picks the best ISA variant via
/// USE_MULTITARGET_CODE dispatch, then falls through to decodeDispatch.
ALWAYS_INLINE void decode(
    const char * src,
    char       * dst,
    int64_t num_elements,
    UInt8  element_bytes)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::AVX512BW))
    {
        decodeDispatchAVX512BW(src, dst, num_elements, element_bytes);
        return;
    }
    if (isArchSupported(TargetArch::AVX512F))
    {
        decodeDispatchAVX512F(src, dst, num_elements, element_bytes);
        return;
    }
    if (isArchSupported(TargetArch::AVX2))
    {
        decodeDispatchAVX2(src, dst, num_elements, element_bytes);
        return;
    }
    if (isArchSupported(TargetArch::SSE42))
    {
        decodeDispatchSSE42(src, dst, num_elements, element_bytes);
        return;
    }
#endif
    decodeDispatch(src, dst, num_elements, element_bytes);
}

UInt8 getElementBytesSize(const IDataType * column_type)
{
    if (!column_type->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Codec ByteStreamSplit is not applicable for {} because the data "
            "type is not of fixed size",
            column_type->getName());

    size_t size = column_type->getSizeOfValueInMemory();

    if (size < 2)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Codec ByteStreamSplit is not applicable for {} — element size "
            "must be at least 2 bytes (splitting 1-byte values produces only "
            "one stream and has no effect)",
            column_type->getName());

    if (size > MAX_ELEMENT_WIDTH)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Codec ByteStreamSplit is not applicable for {} — element size "
            "{} exceeds the maximum supported width of {} bytes",
            column_type->getName(), size, MAX_ELEMENT_WIDTH);

    return static_cast<UInt8>(size);
}

} // anonymous namespace


CompressionCodecByteStreamSplit::CompressionCodecByteStreamSplit(UInt8 element_bytes_size_)
    : element_bytes_size(element_bytes_size_)
{
    setCodecDescription(
        "ByteStreamSplit",
        {make_intrusive<ASTLiteral>(static_cast<UInt64>(element_bytes_size))});
}

uint8_t CompressionCodecByteStreamSplit::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::ByteStreamSplit);
}

void CompressionCodecByteStreamSplit::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash, /*ignore_aliases=*/ true);
}

UInt32 CompressionCodecByteStreamSplit::doCompressData(
    const char * source, UInt32 source_size, char * dest) const
{
    UInt8 bytes_to_skip = source_size % element_bytes_size;

    dest[0] = element_bytes_size;
    dest[1] = bytes_to_skip;

    if (bytes_to_skip)
        memcpy(dest + HEADER_SIZE, source, bytes_to_skip);

    const char * aligned_source = source + bytes_to_skip;
    char       * body_dest      = dest   + HEADER_SIZE + bytes_to_skip;
    UInt32       aligned_size   = source_size - bytes_to_skip;
    int64_t      num_elements   = static_cast<int64_t>(aligned_size / element_bytes_size);

    if (num_elements > 0)
        encode(aligned_source, body_dest, num_elements, element_bytes_size);

    return HEADER_SIZE + source_size;
}

UInt32 CompressionCodecByteStreamSplit::doDecompressData(
    const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    if (source_size < HEADER_SIZE)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress ByteStreamSplit-encoded data: source too small ({})",
            source_size);

    if (uncompressed_size == 0)
        return 0;

    UInt8 saved_element_bytes = static_cast<UInt8>(source[0]);
    UInt8 bytes_to_skip       = static_cast<UInt8>(source[1]);

    if (saved_element_bytes == 0)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress ByteStreamSplit-encoded data: invalid element size 0 in header");

    if (bytes_to_skip >= saved_element_bytes)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress ByteStreamSplit-encoded data: bytes_to_skip ({}) >= element_bytes ({})",
            static_cast<UInt32>(bytes_to_skip), static_cast<UInt32>(saved_element_bytes));

    if (source_size < static_cast<UInt32>(HEADER_SIZE + bytes_to_skip))
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress ByteStreamSplit-encoded data: source too small for header + tail");

    if (bytes_to_skip)
        memcpy(dest, source + HEADER_SIZE, bytes_to_skip);

    UInt32 aligned_uncompressed = uncompressed_size - bytes_to_skip;

    if (aligned_uncompressed == 0)
        return bytes_to_skip;

    if (aligned_uncompressed % saved_element_bytes != 0)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress ByteStreamSplit-encoded data: aligned uncompressed size ({}) "
            "is not a multiple of element size ({})",
            aligned_uncompressed, static_cast<UInt32>(saved_element_bytes));

    int64_t      num_elements = static_cast<int64_t>(aligned_uncompressed / saved_element_bytes);
    const char * body_src     = source + HEADER_SIZE + bytes_to_skip;
    char       * aligned_dest = dest   + bytes_to_skip;

    decode(body_src, aligned_dest, num_elements, saved_element_bytes);

    return uncompressed_size;
}



void registerCodecByteStreamSplit(CompressionCodecFactory & factory)
{
    UInt8 method_code = static_cast<uint8_t>(CompressionMethodByte::ByteStreamSplit);

    auto codec_builder = [&](const ASTPtr & arguments, const IDataType * column_type) -> CompressionCodecPtr
    {
        if (arguments && !arguments->children.empty())
        {
            if (arguments->children.size() > 1)
                throw Exception(
                    ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE,
                    "ByteStreamSplit codec accepts at most 1 parameter (element "
                    "byte width), given {}",
                    arguments->children.size());

            const auto * literal = arguments->children[0]->as<ASTLiteral>();
            if (!literal || literal->value.getType() != Field::Types::Which::UInt64)
                throw Exception(
                    ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                    "ByteStreamSplit codec argument must be a positive integer "
                    "specifying the element byte width (e.g. 4 for Float32/Int32, "
                    "8 for Float64/Int64, 16 for UUID/IPv6/Int128)");

            UInt64 user_size = literal->value.safeGet<UInt64>();

            if (user_size < 2)
                throw Exception(
                    ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                    "ByteStreamSplit element byte width must be at least 2 "
                    "(splitting 1-byte values produces only one stream and has "
                    "no effect), given {}",
                    user_size);

            if (user_size > MAX_ELEMENT_WIDTH)
                throw Exception(
                    ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                    "ByteStreamSplit element byte width must be at most {}, "
                    "given {}",
                    MAX_ELEMENT_WIDTH, user_size);

            return std::make_shared<CompressionCodecByteStreamSplit>(
                static_cast<UInt8>(user_size));
        }

        if (column_type)
            return std::make_shared<CompressionCodecByteStreamSplit>(
                getElementBytesSize(column_type));

        return std::make_shared<CompressionCodecByteStreamSplit>(4);
    };

    factory.registerCompressionCodecWithType("ByteStreamSplit", method_code, codec_builder);
}

} // namespace DB
