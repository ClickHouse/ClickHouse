#include <gtest/gtest.h>

#include <Compression/Pcodec/Constants.h>
#include <Compression/Pcodec/StandaloneDecoder.h>
#include <Compression/Pcodec/StandaloneEncoder.h>

#include <cstdint>
#include <cstring>
#include <vector>

/** Exercises the no-expansion guarantee of `encodeStandaloneInto`: the output of the standalone PCO
  * encoder never exceeds `encodeStandaloneMaxSize`, regardless of the input, because each chunk that
  * would expand past the per-chunk no-expansion bound is re-encoded with the trivial configuration
  * (Classic mode, no delta, a single bin) that is exactly bounded. The decode-only fixture tests in
  * `gtest_pcodec_fixtures` cover modes the encoder never produces but never cross the multi-chunk or
  * trivial-fallback paths, so those are covered here instead.
  */

using namespace DB::Pcodec;

namespace
{

/// Knuth MMIX LCG, matching the generator used by `gtest_pcodec_fixtures`.
struct Lcg
{
    uint64_t state;
    explicit Lcg(uint64_t seed) : state(seed) {}
    uint64_t next()
    {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        return state;
    }
};

/// Encodes `values`, asserts the encoded size honours `encodeStandaloneMaxSize`, decodes it back and
/// asserts a bit-exact round-trip. Returns the number of chunks that took the trivial fallback.
template <typename T>
size_t encodeCheckRoundTrip(const std::vector<T> & values, size_t compression_level)
{
    const size_t n = values.size();
    const size_t max_size = encodeStandaloneMaxSize<T>(n);

    std::vector<uint8_t> encoded(max_size);
    size_t trivial_fallback_chunks = 0;
    const size_t encoded_size = encodeStandaloneInto<T>(
        reinterpret_cast<const uint8_t *>(values.data()), n, encoded.data(), compression_level, &trivial_fallback_chunks);

    /// The core contract: the no-expansion bound is never exceeded.
    EXPECT_LE(encoded_size, max_size);

    /// The decoder requires DECODE_BATCH_OVERSHOOT readable slack bytes after the payload.
    std::vector<uint8_t> padded(encoded_size + DECODE_BATCH_OVERSHOOT);
    std::memcpy(padded.data(), encoded.data(), encoded_size);

    std::vector<T> decoded(n);
    const size_t written = decodeStandalone(
        padded.data(), encoded_size, reinterpret_cast<uint8_t *>(decoded.data()), n * sizeof(T), sizeof(T));

    EXPECT_EQ(written, n * sizeof(T));
    EXPECT_EQ(std::memcmp(decoded.data(), values.data(), n * sizeof(T)), 0);

    return trivial_fallback_chunks;
}

/// Builds an array whose every `ENCODE_CHUNK_N` chunk starts with a short run of clean multiples of
/// `base` (the bait that makes the sample-based mode estimate pick `IntMult`) followed by an
/// incompressible tail (which makes that mode expand past the no-expansion bound).
template <typename T>
std::vector<T> makeModeBait(size_t n, uint64_t base, size_t prefix_per_chunk)
{
    std::vector<T> values(n);
    Lcg lcg(0x123456789ABCDEFULL);
    for (size_t i = 0; i < n; ++i)
    {
        const size_t offset_in_chunk = i % ENCODE_CHUNK_N;
        if (offset_in_chunk < prefix_per_chunk)
            values[i] = static_cast<T>(base * (offset_in_chunk % 50));
        else
            values[i] = static_cast<T>(lcg.next());
    }
    return values;
}

}

/// Incompressible data spanning several `ENCODE_CHUNK_N` chunks: every chunk is at the no-expansion
/// limit, so this stresses the per-chunk framing accounting in `encodeStandaloneMaxSize`.
TEST(CodecPcoEncodeBounds, IncompressibleMultiChunkRoundTrip)
{
    const size_t n = ENCODE_CHUNK_N * 2 + 12345;

    Lcg lcg(0xC0FFEEULL);
    std::vector<uint64_t> values_u64(n);
    for (auto & x : values_u64)
        x = lcg.next();
    encodeCheckRoundTrip<uint64_t>(values_u64, DEFAULT_COMPRESSION_LEVEL);

    std::vector<uint32_t> values_u32(n);
    for (auto & x : values_u32)
        x = static_cast<uint32_t>(lcg.next());
    encodeCheckRoundTrip<uint32_t>(values_u32, DEFAULT_COMPRESSION_LEVEL);

    std::vector<double> values_f64(n);
    for (auto & x : values_f64)
    {
        uint64_t bits = lcg.next();
        std::memcpy(&x, &bits, sizeof(x));
    }
    encodeCheckRoundTrip<double>(values_f64, DEFAULT_COMPRESSION_LEVEL);
}

/// Forces the trivial fallback branch: each chunk starts with a short run of clean multiples of a
/// small base, so the cheap sample-based mode estimate picks the `IntMult` mode, but the
/// incompressible tail of the chunk makes that mode expand far past the per-chunk no-expansion
/// bound (by ~32 KiB at this compression level — a wide, structural margin, not a borderline one).
/// The encoder must detect the expansion and re-encode each such chunk trivially, keeping the
/// output within `encodeStandaloneMaxSize` and round-tripping exactly.
TEST(CodecPcoEncodeBounds, TrivialFallbackOnModeMisprediction)
{
    /// A low compression level uses coarse bins, under which the mispredicted `IntMult` split of the
    /// incompressible tail genuinely expands; higher levels compress it back under the bound.
    const size_t compression_level = 1;
    const uint64_t base = 3;
    const size_t prefix_per_chunk = 30000;

    /// Three full chunks (each with a bait prefix + incompressible tail) plus a short trailing chunk.
    const size_t n = ENCODE_CHUNK_N * 3 + 1234;

    const size_t fallbacks_u64
        = encodeCheckRoundTrip<uint64_t>(makeModeBait<uint64_t>(n, base, prefix_per_chunk), compression_level);
    EXPECT_GT(fallbacks_u64, 0u);

    const size_t fallbacks_u32
        = encodeCheckRoundTrip<uint32_t>(makeModeBait<uint32_t>(n, base, prefix_per_chunk), compression_level);
    EXPECT_GT(fallbacks_u32, 0u);
}
