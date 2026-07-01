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

/// Builds a single-value standalone `.pco` stream whose `IntMult` chunk reconstructs
/// `value = primary * base + secondary` with `primary = primary_latent`, `base = base_latent` and a
/// constant-zero secondary. A well-formed `IntMult` stream always satisfies `primary * base + secondary`
/// equal to the (in-range) value, so `primary * base` never exceeds the latent width; only a malformed
/// stream (whose metadata lies) can make both factors large at once, or set `base == 0`. Passing such
/// values here fabricates a malformed stream to pin the decoder's fail-closed guard. The byte layout
/// mirrors the real encoder's output for a one-value chunk with a single-bin primary (a full-width
/// offset, `lower = 0`) and a constant-zero secondary latent variable.
template <typename T>
std::vector<uint8_t> buildIntMultStream(uint64_t base_latent, uint64_t primary_latent)
{
    using L = typename NumberTraits<T>::Latent;
    constexpr Bitlen l_bits = latentBits<L>;
    const auto type_byte = static_cast<uint8_t>(NumberTraits<T>::type_byte);

    /// Over-sized so the BitWriter's up-to-16-byte write-past-the-end slack stays in bounds.
    std::vector<uint8_t> buf(256, 0);
    BitWriter writer(buf.data(), buf.size());

    /// --- standalone header ---
    writer.writeAlignedBytes(MAGIC_HEADER.data(), MAGIC_HEADER.size());
    writer.writeU64(CURRENT_STANDALONE_VERSION, BITS_TO_ENCODE_STANDALONE_VERSION);
    writer.writeAlignedBytes(&type_byte, 1); // uniform type
    writeVarint(writer, 1); // n hint
    writer.finishByte();

    /// --- wrapped header (format 4.1) ---
    const uint8_t version_bytes[2] = {4, 1};
    writer.writeAlignedBytes(version_bytes, 2);

    /// --- chunk preamble ---
    writer.writeAlignedBytes(&type_byte, 1);
    writer.writeU64(0, BITS_TO_ENCODE_N_ENTRIES); // n - 1 == 0 (one value)

    /// --- chunk metadata: IntMult mode, no delta, primary + secondary latent-var metas ---
    writer.writeU64(1, BITS_TO_ENCODE_MODE_VARIANT); // 1 == IntMult
    writer.writeU64(base_latent, l_bits);
    writer.writeU64(0, BITS_TO_ENCODE_DELTA_ENCODING_VARIANT); // 0 == None
    PcoArray<Bin> primary_bins(1);
    primary_bins[0] = Bin{/*weight=*/1, /*lower=*/0, /*offset_bits=*/l_bits};
    writeChunkLatentVarMeta(writer, /*ans_size_log=*/0, primary_bins, l_bits);
    PcoArray<Bin> secondary_bins(1);
    secondary_bins[0] = Bin{/*weight=*/1, /*lower=*/0, /*offset_bits=*/0};
    writeChunkLatentVarMeta(writer, /*ans_size_log=*/0, secondary_bins, l_bits);
    writer.finishByte();

    /// --- page metadata: no delta moments, and the 4 ANS final states are 0 bits each (ans_size_log == 0) ---
    writer.finishByte();

    /// --- page body: the single primary offset (full latent width); the secondary is constant zero ---
    writer.writeU64(primary_latent, l_bits);
    writer.finishByte();

    /// --- footer terminator ---
    const uint8_t term = MAGIC_TERMINATION_BYTE;
    writer.writeAlignedBytes(&term, 1);

    buf.resize(writer.byteSize());
    return buf;
}

/// Builds a single-value standalone `.pco` stream whose `FloatQuant` chunk reconstructs one float from
/// `primary = primary_latent` and a constant secondary `m = secondary_latent`, quantized by `k` bits.
/// A well-formed `FloatQuant` stream splits a latent `u` as `primary = u >> k` (so `primary` fits in
/// `latentBits - k` bits) and a remainder `m` in `[0, (1 << k) - 1]`; a malformed stream can violate
/// either bound, which would fabricate a float instead of failing closed. Passing out-of-range
/// `primary_latent` / `secondary_latent` here pins the decoder's fail-closed guard. The byte layout
/// mirrors the real encoder's output for a one-value chunk with a single-bin primary (a full-width
/// offset, `lower = 0`) and a constant secondary latent variable (offset width 0, `lower = m`).
template <typename T>
std::vector<uint8_t> buildFloatQuantStream(Bitlen k, uint64_t primary_latent, uint64_t secondary_latent)
{
    using L = typename NumberTraits<T>::Latent;
    constexpr Bitlen l_bits = latentBits<L>;
    const auto type_byte = static_cast<uint8_t>(NumberTraits<T>::type_byte);

    std::vector<uint8_t> buf(256, 0);
    BitWriter writer(buf.data(), buf.size());

    /// --- standalone header ---
    writer.writeAlignedBytes(MAGIC_HEADER.data(), MAGIC_HEADER.size());
    writer.writeU64(CURRENT_STANDALONE_VERSION, BITS_TO_ENCODE_STANDALONE_VERSION);
    writer.writeAlignedBytes(&type_byte, 1); // uniform type
    writeVarint(writer, 1); // n hint
    writer.finishByte();

    /// --- wrapped header (format 4.1) ---
    const uint8_t version_bytes[2] = {4, 1};
    writer.writeAlignedBytes(version_bytes, 2);

    /// --- chunk preamble ---
    writer.writeAlignedBytes(&type_byte, 1);
    writer.writeU64(0, BITS_TO_ENCODE_N_ENTRIES); // n - 1 == 0 (one value)

    /// --- chunk metadata: FloatQuant mode, no delta, primary + secondary latent-var metas ---
    writer.writeU64(3, BITS_TO_ENCODE_MODE_VARIANT); // 3 == FloatQuant
    writer.writeU64(k, BITS_TO_ENCODE_QUANTIZE_K);
    writer.writeU64(0, BITS_TO_ENCODE_DELTA_ENCODING_VARIANT); // 0 == None
    PcoArray<Bin> primary_bins(1);
    primary_bins[0] = Bin{/*weight=*/1, /*lower=*/0, /*offset_bits=*/l_bits};
    writeChunkLatentVarMeta(writer, /*ans_size_log=*/0, primary_bins, l_bits);
    PcoArray<Bin> secondary_bins(1);
    secondary_bins[0] = Bin{/*weight=*/1, /*lower=*/secondary_latent, /*offset_bits=*/0};
    writeChunkLatentVarMeta(writer, /*ans_size_log=*/0, secondary_bins, l_bits);
    writer.finishByte();

    /// --- page metadata: no delta moments, and the 4 ANS final states are 0 bits each (ans_size_log == 0) ---
    writer.finishByte();

    /// --- page body: the single primary offset (full latent width); the secondary is constant ---
    writer.writeU64(primary_latent, l_bits);
    writer.finishByte();

    /// --- footer terminator ---
    const uint8_t term = MAGIC_TERMINATION_BYTE;
    writer.writeAlignedBytes(&term, 1);

    buf.resize(writer.byteSize());
    return buf;
}

/// Decodes one hand-built standalone stream into a single value of type T.
template <typename T>
T decodeSingle(const std::vector<uint8_t> & stream)
{
    std::vector<uint8_t> padded(stream.size() + DECODE_BATCH_OVERSHOOT, 0);
    std::memcpy(padded.data(), stream.data(), stream.size());
    T out{};
    size_t written = decodeStandalone(padded.data(), stream.size(), reinterpret_cast<uint8_t *>(&out), sizeof(T), sizeof(T));
    EXPECT_EQ(written, sizeof(T));
    return out;
}

}

/// A malformed `IntMult` stream must fail closed rather than fabricate a value by wrapping. The
/// decomposition `value = primary * base + secondary` is only valid when `base != 0`,
/// `secondary < base`, and `primary * base + secondary` fits the latent width; the decoder rejects a
/// stream that violates any of these instead of returning the modular wrap. A checksummed `0x9d`
/// frame reaches this decoder through the shared `CompressedReadBuffer`, so this is a current
/// fail-closed boundary. The overflow test itself stays defined even for sub-word latents
/// (`U16`/`I16`): `primary * base` for `primary = base = 65535` is `0xFFFE0001`, which would overflow
/// the signed `int` both `uint16_t` operands promote to, so the check forms the product in a wider
/// unsigned accumulator — a sanitizer build passes these `EXPECT_THROW`s without trapping.
TEST(CodecPcoEncodeBounds, IntMultDecompositionFailsClosed)
{
    /// A well-formed single-value stream (base = 3, primary = 2, secondary = 0 -> value 6) decodes
    /// cleanly: the guard never rejects a valid decomposition.
    uint16_t good = 0;
    ASSERT_NO_THROW(good = decodeSingle<uint16_t>(buildIntMultStream<uint16_t>(3, 2)));
    EXPECT_EQ(good, 6);

    /// `base == 0` is never a valid IntMult base.
    EXPECT_THROW(decodeSingle<uint16_t>(buildIntMultStream<uint16_t>(0, 5)), PcodecError);

    /// `primary * base` overflowing the latent width fabricates a value; reject it.
    for (uint64_t v : {uint64_t{65535}, uint64_t{65534}, uint64_t{50000}, uint64_t{40000}})
    {
        EXPECT_THROW(decodeSingle<uint16_t>(buildIntMultStream<uint16_t>(v, v)), PcodecError) << "base = primary = " << v;
        EXPECT_THROW(decodeSingle<int16_t>(buildIntMultStream<int16_t>(v, v)), PcodecError) << "base = primary = " << v;

        const uint64_t v8 = v & 0xFF;
        EXPECT_THROW(decodeSingle<uint8_t>(buildIntMultStream<uint8_t>(v8, v8)), PcodecError) << "base = primary = " << v8;
        EXPECT_THROW(decodeSingle<int8_t>(buildIntMultStream<int8_t>(v8, v8)), PcodecError) << "base = primary = " << v8;
    }

    /// Wider latents (`U32`/`U64`) overflow too: `primary = base` just past the square root of the range.
    EXPECT_THROW(decodeSingle<uint32_t>(buildIntMultStream<uint32_t>(0x10000, 0x10000)), PcodecError);
    EXPECT_THROW(decodeSingle<uint64_t>(buildIntMultStream<uint64_t>(0x100000000ULL, 0x100000000ULL)), PcodecError);
}

/// A malformed `FloatQuant` stream must fail closed rather than fabricate a float. A valid stream
/// splits a latent `u` as `primary = u >> k` (fitting `latentBits - k` bits) and a remainder `m` in
/// `[0, (1 << k) - 1]`; the decoder rejects an oversized `primary` (whose `primary << k` drops its
/// high bits) or an out-of-range `m` (whose `lowest_k_bits_max - m` underflows) instead of returning
/// a wrapped value. Reachable today: a checksummed `0x9d` frame reaches this decoder through the
/// shared `CompressedReadBuffer`.
TEST(CodecPcoEncodeBounds, FloatQuantDecompositionFailsClosed)
{
    /// A well-formed single-value stream decodes cleanly (k = 3; primary and m both in range).
    ASSERT_NO_THROW(decodeSingle<float>(buildFloatQuantStream<float>(/*k=*/3, /*primary_latent=*/5, /*secondary_latent=*/4)));
    ASSERT_NO_THROW(decodeSingle<double>(buildFloatQuantStream<double>(/*k=*/3, /*primary_latent=*/5, /*secondary_latent=*/4)));

    /// `m` above `(1 << k) - 1` is out of range: with k = 1 the only valid `m` are 0 and 1.
    EXPECT_THROW(decodeSingle<float>(buildFloatQuantStream<float>(/*k=*/1, /*primary_latent=*/0, /*secondary_latent=*/5)), PcodecError);
    EXPECT_THROW(decodeSingle<double>(buildFloatQuantStream<double>(/*k=*/1, /*primary_latent=*/0, /*secondary_latent=*/5)), PcodecError);

    /// `primary` occupying more than `latentBits - k` bits makes `primary << k` drop its high bits.
    EXPECT_THROW(decodeSingle<float>(buildFloatQuantStream<float>(/*k=*/1, /*primary_latent=*/0x80000000u, /*secondary_latent=*/0)), PcodecError);
    EXPECT_THROW(decodeSingle<double>(buildFloatQuantStream<double>(/*k=*/1, /*primary_latent=*/0x8000000000000000ULL, /*secondary_latent=*/0)), PcodecError);
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
