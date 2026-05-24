/// TurboQuant compression codec for high-dimensional Float32 vectors.
///
/// Based on: https://github.com/0xSero/turboquant
///   "TurboQuant: Quantization for Vectors"
///
/// Algorithm
/// ---------
/// For each vector of `dim` Float32 values:
///   1. Compute the L2 norm; normalise to a unit vector.
///   2. Subsampled Randomised Hadamard Transform (SRHT):
///        a. XOR-flip the IEEE 754 sign bits with a fixed random mask
///           (equivalent to multiplying by a diagonal ±1 matrix D).
///        b. Apply the unnormalised Fast Walsh-Hadamard Transform (FWHT).
///      After the SRHT each rotated coordinate is distributed as N(0,1)
///      for unit-norm input, allowing the pre-computed codebook to apply
///      directly without any per-vector scaling.
///   3. Quantise each coordinate to `bits` bits using a Lloyd-Max codebook
///      that is optimal for N(0,1).
///   4. Pack the indices into ceil(bits*dim/8) bytes.
///
/// Decompression is the exact inverse:
///   unpack → look up centroid → zero-pad → FWHT → sign-flip → ÷ padded_dim → × norm.
///
/// The FWHT is intentionally unnormalised.  The forward transform produces
/// coordinates ~N(0,1) without any extra scaling pass, and the inverse
/// only needs a single division by padded_dim.  This avoids two O(padded_dim)
/// scalar passes per vector compared to using a normalised transform.
///
/// SIMD acceleration (AVX2 where available):
///   * WHT butterfly pairs for len >= 8
///   * XOR sign-flip (8 floats per instruction via integer XOR on sign bits)
///   * Quantisation (8 values × all boundaries in parallel)
///   * A scalar path covers all other architectures and small len values.
///
/// Syntax: CODEC(TurboQuant(dim, bits))
///   dim   -- number of Float32 elements per vector (2 … 65536)
///   bits  -- bits per element: 1, 2, 3, or 4
///
/// Compression ratio ≈ 32 / bits for large dim (plus 4 bytes of norm overhead
/// per vector).
///
/// Compressed block layout
/// -----------------------
///   [1 byte]  bits per element (for sanity-check at decompression)
///   [1 byte]  reserved (0)
///   For each complete vector of `dim` Float32 elements:
///     [4 bytes]                 L2 norm of the original vector (Float32 LE)
///     [ceil(bits*dim/8) bytes]  packed b-bit quantisation indices (LSB first)
///   [remainder bytes]           trailing bytes that do not form a complete
///                               vector, copied verbatim

#ifdef __AVX2__
#    include <immintrin.h>
#endif

#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <Compression/ICompressionCodec.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <bit>
#include <cmath>
#include <cstring>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_CODEC_PARAMETER;
    extern const int CANNOT_DECOMPRESS;
    extern const int CORRUPTED_DATA;
}


// ============================================================
//  Codec class
// ============================================================

class CompressionCodecTurboQuant final : public ICompressionCodec
{
public:
    CompressionCodecTurboQuant(UInt32 vector_dim_, UInt8 bits_);

    uint8_t getMethodByte() const override;
    void updateHash(SipHash & hash) const override;
    String getDescription() const override;
    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return false; }

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
    UInt32 doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;
    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

private:
    /// Lloyd-Max centroids for N(0,1), indexed [bits-1][centroid].
    static const float CENTROIDS[4][16];
    /// Decision boundaries (midpoints between centroids), indexed [bits-1][boundary].
    static const float BOUNDARIES[4][15];

    static constexpr UInt32 HEADER_SIZE = 2;

    UInt32 vector_dim;  /// d
    UInt8  bits;        /// b  (1–4)
    UInt32 padded_dim;  /// smallest power of two >= vector_dim

    /// XOR masks for the sign-flip step of the SRHT (forward pass).
    /// sign_masks[i] = 0x80000000 when the random sign is −1, else 0.
    std::vector<UInt32> sign_masks;

    /// Scaled signs for the inverse pass: ±1 / padded_dim.
    /// Applied as a plain float multiply after the inverse FWHT.
    std::vector<float> signs_scaled;

    static UInt32 nextPow2(UInt32 n);
    void initSignTables();

    /// Unnormalised in-place Fast Walsh-Hadamard Transform.
    /// n must be a power of two.  Self-inverse up to a factor of n:
    ///   fwht(fwht(x)) == n * x
    static void fwht(float * x, UInt32 n);

    /// Quantise z (already in N(0,1) scale) to a b-bit index.
    UInt8 quantize(float z) const;

    /// Return the N(0,1)-scaled centroid for a b-bit index.
    float dequantize(UInt8 idx) const;

    /// Pack / unpack b-bit indices using specialised code for each b.
    static void packBits(const UInt8 * indices, UInt32 count, UInt8 b, uint8_t * dest);
    static void unpackBits(const uint8_t * src, UInt32 count, UInt8 b, UInt8 * indices);

    /// SIMD-accelerated quantisation of `count` floats from `buf` into `indices`.
    void quantizeBlock(const float * buf, UInt8 * indices, UInt32 count) const;
};


// ============================================================
//  Static tables
// ============================================================

// clang-format off
const float CompressionCodecTurboQuant::CENTROIDS[4][16] =
{
    // b=1 — 2 centroids
    { -0.7979f, 0.7979f,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
    // b=2 — 4 centroids
    { -1.5104f, -0.4528f, 0.4528f, 1.5104f,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
    // b=3 — 8 centroids
    { -2.1520f, -1.3439f, -0.7560f, -0.2451f,
       0.2451f,  0.7560f,  1.3439f,  2.1520f,
      0, 0, 0, 0, 0, 0, 0, 0 },
    // b=4 — 16 centroids
    { -2.7326f, -2.0690f, -1.6181f, -1.2562f,
      -0.9424f, -0.6567f, -0.3880f, -0.1284f,
       0.1284f,  0.3880f,  0.6567f,  0.9424f,
       1.2562f,  1.6181f,  2.0690f,  2.7326f },
};

const float CompressionCodecTurboQuant::BOUNDARIES[4][15] =
{
    // b=1 — 1 boundary
    { 0.0f,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
    // b=2 — 3 boundaries
    { -0.9816f, 0.0f, 0.9816f,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
    // b=3 — 7 boundaries
    { -1.7480f, -1.0500f, -0.5006f, 0.0f, 0.5006f, 1.0500f, 1.7480f,
      0, 0, 0, 0, 0, 0, 0, 0 },
    // b=4 — 15 boundaries
    { -2.4008f, -1.8436f, -1.4372f, -1.0993f,
      -0.7996f, -0.5224f, -0.2582f,  0.0f,
       0.2582f,  0.5224f,  0.7996f,  1.0993f,
       1.4372f,  1.8436f,  2.4008f },
};
// clang-format on


// ============================================================
//  Helpers
// ============================================================

UInt32 CompressionCodecTurboQuant::nextPow2(UInt32 n)
{
    if (n <= 1)
        return 1;
    if ((n & (n - 1)) == 0)
        return n;
    return UInt32{1} << (32 - std::countl_zero(n - 1));
}

void CompressionCodecTurboQuant::initSignTables()
{
    sign_masks.resize(padded_dim);
    signs_scaled.resize(padded_dim);

    const float inv_n = 1.0f / static_cast<float>(padded_dim);

    /// xorshift64 with a fixed seed — same sequence on every run.
    UInt64 state = 0x6C62272E07BB0142ULL;
    for (UInt32 i = 0; i < padded_dim; ++i)
    {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;

        const bool negative = (state & 1) == 0;
        sign_masks[i]  = negative ? 0x80000000u : 0u;
        signs_scaled[i] = negative ? -inv_n : inv_n;
    }
}

/// Unnormalised FWHT: fwht(fwht(x)) == n * x.
/// Uses AVX2 butterfly pairs for len >= 8, scalar otherwise.
void CompressionCodecTurboQuant::fwht(float * x, UInt32 n)
{
    for (UInt32 len = 1; len < n; len <<= 1)
    {
        for (UInt32 i = 0; i < n; i += len << 1)
        {
#ifdef __AVX2__
            if (len >= 8)
            {
                for (UInt32 j = 0; j < len; j += 8)
                {
                    __m256 u = _mm256_loadu_ps(x + i + j);
                    __m256 v = _mm256_loadu_ps(x + i + j + len);
                    _mm256_storeu_ps(x + i + j,       _mm256_add_ps(u, v));
                    _mm256_storeu_ps(x + i + j + len, _mm256_sub_ps(u, v));
                }
                continue;
            }
#endif
            for (UInt32 j = 0; j < len; ++j)
            {
                const float u = x[i + j];
                const float v = x[i + j + len];
                x[i + j]       = u + v;
                x[i + j + len] = u - v;
            }
        }
    }
    /// No normalisation here — the caller handles the 1/n factor only where
    /// needed (the decode path), avoiding two superfluous O(n) passes.
}

UInt8 CompressionCodecTurboQuant::quantize(float z) const
{
    const float * bounds = BOUNDARIES[bits - 1];
    const UInt8 num_bounds = static_cast<UInt8>((1u << bits) - 1u);
    UInt8 lo = 0, hi = num_bounds;
    while (lo < hi)
    {
        const UInt8 mid = (lo + hi) / 2;
        if (z < bounds[mid])
            hi = mid;
        else
            lo = mid + 1;
    }
    return lo;
}

float CompressionCodecTurboQuant::dequantize(UInt8 idx) const
{
    return CENTROIDS[bits - 1][idx];
}

/// Quantise a block of floats.  Processes 8 at a time via AVX2; scalar tail.
void CompressionCodecTurboQuant::quantizeBlock(const float * buf, UInt8 * indices, UInt32 count) const
{
#ifdef __AVX2__
    const UInt8 nb = static_cast<UInt8>((1u << bits) - 1u);
    const float * bounds = BOUNDARIES[bits - 1];

    const UInt32 simd_end = (count / 8) * 8;
    for (UInt32 i = 0; i < simd_end; i += 8)
    {
        const __m256 vals = _mm256_loadu_ps(buf + i);
        __m256i idx = _mm256_setzero_si256();
        for (UInt8 k = 0; k < nb; ++k)
        {
            /// cmp lane = 0xFFFFFFFF (== int32 −1) if vals[lane] >= bound, else 0.
            /// Subtracting −1 increments the index by 1 for each exceeded boundary.
            const __m256 cmp = _mm256_cmp_ps(vals, _mm256_set1_ps(bounds[k]), _CMP_GE_OQ);
            idx = _mm256_sub_epi32(idx, _mm256_castps_si256(cmp));
        }
        alignas(32) int32_t tmp[8];
        _mm256_store_si256(reinterpret_cast<__m256i *>(tmp), idx);
        for (int j = 0; j < 8; ++j)
            indices[i + j] = static_cast<UInt8>(tmp[j]);
    }
    for (UInt32 i = simd_end; i < count; ++i)
        indices[i] = quantize(buf[i]);
#else
    for (UInt32 i = 0; i < count; ++i)
        indices[i] = quantize(buf[i]);
#endif
}


// ============================================================
//  Bit packing / unpacking  (specialised per b for speed)
// ============================================================

void CompressionCodecTurboQuant::packBits(
    const UInt8 * indices, UInt32 count, UInt8 b, uint8_t * dest)
{
    switch (b)
    {
        case 1:
        {
            const UInt32 full = count / 8;
            for (UInt32 i = 0; i < full; ++i)
            {
                const UInt8 * s = indices + i * 8;
                dest[i] = static_cast<uint8_t>(s[0] | s[1]<<1 | s[2]<<2 | s[3]<<3
                        | s[4]<<4 | s[5]<<5 | s[6]<<6 | s[7]<<7);
            }
            const UInt32 rem = count % 8;
            if (rem)
            {
                uint8_t byte = 0;
                for (UInt32 j = 0; j < rem; ++j)
                    byte |= indices[full * 8 + j] << j;
                dest[full] = byte;
            }
            break;
        }
        case 2:
        {
            const UInt32 full = count / 4;
            for (UInt32 i = 0; i < full; ++i)
            {
                const UInt8 * s = indices + i * 4;
                dest[i] = static_cast<uint8_t>(s[0] | s[1]<<2 | s[2]<<4 | s[3]<<6);
            }
            const UInt32 rem = count % 4;
            if (rem)
            {
                uint8_t byte = 0;
                for (UInt32 j = 0; j < rem; ++j)
                    byte |= indices[full * 4 + j] << (j * 2);
                dest[full] = byte;
            }
            break;
        }
        case 3:
        {
            /// 8 three-bit indices pack perfectly into 3 bytes (24 bits).
            const UInt32 full = count / 8;
            for (UInt32 i = 0; i < full; ++i)
            {
                const UInt8 * s = indices + i * 8;
                const UInt32 lo = s[0] | s[1]<<3 | s[2]<<6 | s[3]<<9
                                | s[4]<<12 | s[5]<<15 | s[6]<<18 | s[7]<<21;
                dest[i * 3 + 0] = static_cast<uint8_t>(lo);
                dest[i * 3 + 1] = static_cast<uint8_t>(lo >> 8);
                dest[i * 3 + 2] = static_cast<uint8_t>(lo >> 16);
            }
            /// Tail (< 8 indices) via generic bit-write.
            UInt32 bit_pos = full * 24;
            for (UInt32 i = full * 8; i < count; ++i)
            {
                const UInt8 idx = indices[i];
                for (UInt8 k = 0; k < 3; ++k, ++bit_pos)
                    if ((idx >> k) & 1u)
                        dest[bit_pos / 8] |= static_cast<uint8_t>(1u << (bit_pos % 8));
            }
            break;
        }
        case 4:
        {
            const UInt32 full = count / 2;
            for (UInt32 i = 0; i < full; ++i)
                dest[i] = static_cast<uint8_t>(indices[i * 2] | indices[i * 2 + 1]<<4);
            if (count & 1)
                dest[full] = indices[count - 1];
            break;
        }
        default:
            break;
    }
}

void CompressionCodecTurboQuant::unpackBits(
    const uint8_t * src, UInt32 count, UInt8 b, UInt8 * indices)
{
    switch (b)
    {
        case 1:
        {
            const UInt32 full = count / 8;
            for (UInt32 i = 0; i < full; ++i)
            {
                const uint8_t byte = src[i];
                UInt8 * d = indices + i * 8;
                d[0] = byte & 1; d[1] = (byte>>1)&1; d[2] = (byte>>2)&1; d[3] = (byte>>3)&1;
                d[4] = (byte>>4)&1; d[5] = (byte>>5)&1; d[6] = (byte>>6)&1; d[7] = (byte>>7)&1;
            }
            const UInt32 rem = count % 8;
            for (UInt32 j = 0; j < rem; ++j)
                indices[full * 8 + j] = (src[full] >> j) & 1u;
            break;
        }
        case 2:
        {
            const UInt32 full = count / 4;
            for (UInt32 i = 0; i < full; ++i)
            {
                const uint8_t byte = src[i];
                UInt8 * d = indices + i * 4;
                d[0] = byte & 3; d[1] = (byte>>2)&3; d[2] = (byte>>4)&3; d[3] = (byte>>6)&3;
            }
            const UInt32 rem = count % 4;
            for (UInt32 j = 0; j < rem; ++j)
                indices[full * 4 + j] = (src[full] >> (j * 2)) & 3u;
            break;
        }
        case 3:
        {
            const UInt32 full = count / 8;
            for (UInt32 i = 0; i < full; ++i)
            {
                const UInt32 lo = static_cast<UInt32>(src[i*3])
                                | static_cast<UInt32>(src[i*3+1]) << 8
                                | static_cast<UInt32>(src[i*3+2]) << 16;
                UInt8 * d = indices + i * 8;
                d[0] = lo & 7; d[1] = (lo>>3)&7; d[2] = (lo>>6)&7; d[3] = (lo>>9)&7;
                d[4] = (lo>>12)&7; d[5] = (lo>>15)&7; d[6] = (lo>>18)&7; d[7] = (lo>>21)&7;
            }
            UInt32 bit_pos = full * 24;
            for (UInt32 i = full * 8; i < count; ++i, bit_pos += 3)
            {
                UInt8 idx = 0;
                for (UInt8 k = 0; k < 3; ++k)
                    if ((src[(bit_pos + k) / 8] >> ((bit_pos + k) % 8)) & 1u)
                        idx |= static_cast<UInt8>(1u << k);
                indices[i] = idx;
            }
            break;
        }
        case 4:
        {
            const UInt32 full = count / 2;
            for (UInt32 i = 0; i < full; ++i)
            {
                indices[i * 2]     = src[i] & 0xFu;
                indices[i * 2 + 1] = src[i] >> 4;
            }
            if (count & 1)
                indices[count - 1] = src[full] & 0xFu;
            break;
        }
        default:
            break;
    }
}


// ============================================================
//  Constructor / ICompressionCodec interface
// ============================================================

CompressionCodecTurboQuant::CompressionCodecTurboQuant(UInt32 vector_dim_, UInt8 bits_)
    : vector_dim(vector_dim_)
    , bits(bits_)
    , padded_dim(nextPow2(vector_dim_))
{
    setCodecDescription(
        "TurboQuant",
        {
            make_intrusive<ASTLiteral>(static_cast<UInt64>(vector_dim)),
            make_intrusive<ASTLiteral>(static_cast<UInt64>(bits)),
        });
    initSignTables();
}

uint8_t CompressionCodecTurboQuant::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::TurboQuant);
}

void CompressionCodecTurboQuant::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash, /*ignore_aliases=*/ true);
}

String CompressionCodecTurboQuant::getDescription() const
{
    return fmt::format(
        "TurboQuant: scalar quantisation for {}-dimensional Float32 vectors, {} bit(s) per element.",
        vector_dim, static_cast<unsigned>(bits));
}

UInt32 CompressionCodecTurboQuant::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    const UInt32 num_vectors = uncompressed_size / (4 * vector_dim);
    const UInt32 remainder   = uncompressed_size % (4 * vector_dim);
    const UInt32 per_vector  = 4 + (static_cast<UInt32>(bits) * vector_dim + 7) / 8;
    return HEADER_SIZE + num_vectors * per_vector + remainder;
}


// ============================================================
//  Compress
// ============================================================

UInt32 CompressionCodecTurboQuant::doCompressData(
    const char * source, UInt32 source_size, char * dest) const
{
    dest[0] = static_cast<char>(bits);
    dest[1] = 0;
    char * out = dest + HEADER_SIZE;

    const UInt32 num_vectors  = source_size / (4 * vector_dim);
    const UInt32 remainder    = source_size % (4 * vector_dim);
    const UInt32 packed_bytes = (static_cast<UInt32>(bits) * vector_dim + 7) / 8;

    /// Reuse heap storage across vectors within a single call (and across calls
    /// on the same thread after the first resize).
    static thread_local std::vector<float> buf;
    static thread_local std::vector<UInt8> indices;
    buf.resize(padded_dim);
    indices.resize(vector_dim);

    for (UInt32 vec = 0; vec < num_vectors; ++vec)
    {
        const float * src_vec = reinterpret_cast<const float *>(source + vec * vector_dim * 4);

        /// L2 norm.
        float norm_sq = 0.0f;
        for (UInt32 i = 0; i < vector_dim; ++i)
            norm_sq += src_vec[i] * src_vec[i];
        const float norm = std::sqrt(norm_sq);

        memcpy(out, &norm, 4);
        out += 4;

        /// Normalise into the padded buffer; zero-fill the padding region.
        if (norm > 0.0f)
        {
            const float inv_norm = 1.0f / norm;
            for (UInt32 i = 0; i < vector_dim; ++i)
                buf[i] = src_vec[i] * inv_norm;
        }
        else
        {
            std::fill(buf.begin(), buf.begin() + vector_dim, 0.0f);
        }
        std::fill(buf.begin() + vector_dim, buf.end(), 0.0f);

        /// SRHT forward pass
        /// Step 1 — XOR the sign bit of each float with the random mask.
        /// For a mask entry of 0x80000000 this is identical to multiplying by −1,
        /// but avoids a float multiply entirely.
        {
            auto * buf_u = reinterpret_cast<UInt32 *>(buf.data());
            const UInt32 * sm = sign_masks.data();
#ifdef __AVX2__
            const UInt32 simd_end = (padded_dim / 8) * 8;
            for (UInt32 i = 0; i < simd_end; i += 8)
            {
                __m256i d = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(buf_u + i));
                __m256i m = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(sm + i));
                _mm256_storeu_si256(reinterpret_cast<__m256i *>(buf_u + i), _mm256_xor_si256(d, m));
            }
            for (UInt32 i = simd_end; i < padded_dim; ++i)
                buf_u[i] ^= sm[i];
#else
            for (UInt32 i = 0; i < padded_dim; ++i)
                buf_u[i] ^= sm[i];
#endif
        }

        /// Step 2 — Unnormalised WHT.
        /// After this step, each of the first vector_dim entries is distributed
        /// approximately as N(0, 1) for unit-norm input, so the codebook applies
        /// directly without any additional scaling.
        fwht(buf.data(), padded_dim);

        /// Step 3 — Quantise.
        quantizeBlock(buf.data(), indices.data(), vector_dim);

        /// Step 4 — Pack indices into dest.
        auto * packed = reinterpret_cast<uint8_t *>(out);
        memset(packed, 0, packed_bytes);
        packBits(indices.data(), vector_dim, bits, packed);
        out += packed_bytes;
    }

    if (remainder > 0)
    {
        memcpy(out, source + num_vectors * vector_dim * 4, remainder);
        out += remainder;
    }

    return static_cast<UInt32>(out - dest);
}


// ============================================================
//  Decompress
// ============================================================

UInt32 CompressionCodecTurboQuant::doDecompressData(
    const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    if (source_size < HEADER_SIZE)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS,
            "TurboQuant: compressed data is too small ({} bytes)", source_size);

    const auto stored_bits = static_cast<UInt8>(source[0]);
    if (stored_bits != bits)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "TurboQuant: stored bits per element ({}) does not match codec parameter ({})",
            static_cast<unsigned>(stored_bits), static_cast<unsigned>(bits));

    const char * in = source + HEADER_SIZE;
    char * out = dest;

    const UInt32 num_vectors  = uncompressed_size / (4 * vector_dim);
    const UInt32 remainder    = uncompressed_size % (4 * vector_dim);
    const UInt32 packed_bytes = (static_cast<UInt32>(bits) * vector_dim + 7) / 8;

    static thread_local std::vector<float> buf;
    static thread_local std::vector<UInt8> indices;
    buf.resize(padded_dim);
    indices.resize(vector_dim);

    for (UInt32 vec = 0; vec < num_vectors; ++vec)
    {
        float norm;
        memcpy(&norm, in, 4);
        in += 4;

        /// Unpack b-bit indices.
        unpackBits(reinterpret_cast<const uint8_t *>(in), vector_dim, bits, indices.data());
        in += packed_bytes;

        /// Look up N(0,1) centroids; zero-fill the padding region.
        for (UInt32 i = 0; i < vector_dim; ++i)
            buf[i] = dequantize(indices[i]);
        std::fill(buf.begin() + vector_dim, buf.end(), 0.0f);

        /// Inverse SRHT
        /// Step 1 — Unnormalised WHT (self-inverse up to factor of padded_dim).
        fwht(buf.data(), padded_dim);

        /// Step 2 — Multiply by ±1/padded_dim (precomputed in signs_scaled).
        /// This simultaneously undoes the sign-flip from encoding and divides
        /// by padded_dim to invert the unnormalised WHT.
        {
            const float * sc = signs_scaled.data();
#ifdef __AVX2__
            const UInt32 simd_end = (padded_dim / 8) * 8;
            for (UInt32 i = 0; i < simd_end; i += 8)
            {
                __m256 d = _mm256_loadu_ps(buf.data() + i);
                __m256 s = _mm256_loadu_ps(sc + i);
                _mm256_storeu_ps(buf.data() + i, _mm256_mul_ps(d, s));
            }
            for (UInt32 i = simd_end; i < padded_dim; ++i)
                buf[i] *= sc[i];
#else
            for (UInt32 i = 0; i < padded_dim; ++i)
                buf[i] *= sc[i];
#endif
        }

        /// Scale by the original norm and write.
        auto * out_vec = reinterpret_cast<float *>(out);
        for (UInt32 i = 0; i < vector_dim; ++i)
            out_vec[i] = buf[i] * norm;
        out += vector_dim * 4;
    }

    if (remainder > 0)
    {
        memcpy(out, in, remainder);
        out += remainder;
    }

    return static_cast<UInt32>(out - dest);
}


// ============================================================
//  Registration
// ============================================================

void registerCodecTurboQuant(CompressionCodecFactory & factory)
{
    factory.registerCompressionCodecWithType(
        "TurboQuant",
        static_cast<UInt8>(CompressionMethodByte::TurboQuant),
        [](const ASTPtr & arguments, const IDataType * /*column_type*/) -> CompressionCodecPtr
        {
            if (!arguments || arguments->children.size() != 2)
                throw Exception(
                    ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                    "Codec 'TurboQuant' requires exactly 2 arguments: TurboQuant(dim, bits)");

            const auto * dim_lit = arguments->children[0]->as<ASTLiteral>();
            if (!dim_lit || !isInt64OrUInt64FieldType(dim_lit->value.getType()))
                throw Exception(
                    ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                    "First argument of codec 'TurboQuant' (dim) must be a positive integer");

            const auto * bits_lit = arguments->children[1]->as<ASTLiteral>();
            if (!bits_lit || !isInt64OrUInt64FieldType(bits_lit->value.getType()))
                throw Exception(
                    ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                    "Second argument of codec 'TurboQuant' (bits) must be 1, 2, 3, or 4");

            const auto dim  = dim_lit->value.safeGet<UInt64>();
            const auto bits = bits_lit->value.safeGet<UInt64>();

            if (dim < 2 || dim > 65536)
                throw Exception(
                    ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                    "Codec 'TurboQuant' dim must be in [2, 65536], got {}", dim);

            if (bits < 1 || bits > 4)
                throw Exception(
                    ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                    "Codec 'TurboQuant' bits must be 1, 2, 3, or 4, got {}", bits);

            return std::make_shared<CompressionCodecTurboQuant>(
                static_cast<UInt32>(dim), static_cast<UInt8>(bits));
        });
}

}
