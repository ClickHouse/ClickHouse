#include <Compression/LZ4_decompress_faster.h>

#include <base/types.h>

#include <lz4.h>

#include <gtest/gtest.h>

#include <string>
#include <vector>

/** These tests pin down the behavioural contract of LZ4 decode method 1
  * (`decompressImpl<16, true>` — 16-byte copies with the branchless small-offset
  *  match copy through `copyOverlap<16>`).
  *
  * The variant is reached only when `PerformanceStatistics::choose_method >= 0`,
  * because a fresh `PerformanceStatistics` first samples method 0 and small blocks
  * (< 32 KiB) bypass the bandit straight into `decompressImpl<8>`. So none of the
  * existing codec tests or the fuzzer exercise it. Here we force the variant with
  * `LZ4::PerformanceStatistics{1}` and compare it against the canonical reference
  * decoder (`LZ4_decompress_safe`) for every small offset 1..15 (the branchless
  * `copyOverlap<16>` overlap path) crossed with match lengths that straddle the
  * 16-byte copy granularity, plus truncated and corrupted inputs.
  */

namespace
{

/// Append the LZ4 length-extension bytes for the part of a length that exceeds the
/// 15 already encoded in the token nibble.
void appendLengthExtension(std::vector<UInt8> & out, size_t remainder)
{
    while (remainder >= 255)
    {
        out.push_back(255);
        remainder -= 255;
    }
    out.push_back(static_cast<UInt8>(remainder));
}

/** Hand-encode a raw LZ4 block (no ClickHouse frame, no checksum) consisting of:
  *   sequence 1: `seed` literals, then a match of `match_length` at `offset`;
  *   final sequence: `trailing` literals only (LZ4 blocks always end with literals).
  *
  * Decoding it yields exactly `seed` ++ (match replicated from `offset` back) ++ `trailing`.
  */
std::vector<UInt8> encodeBlock(
    const std::vector<UInt8> & seed,
    UInt16 offset,
    size_t match_length,
    const std::vector<UInt8> & trailing)
{
    std::vector<UInt8> b;

    const size_t lit = seed.size();
    const size_t ml = match_length - 4; /// LZ4 minMatch is 4.

    const UInt8 lit_nibble = static_cast<UInt8>(std::min<size_t>(lit, 15));
    const UInt8 ml_nibble = static_cast<UInt8>(std::min<size_t>(ml, 15));
    b.push_back(static_cast<UInt8>((lit_nibble << 4) | ml_nibble));
    if (lit >= 15)
        appendLengthExtension(b, lit - 15);
    b.insert(b.end(), seed.begin(), seed.end());

    b.push_back(static_cast<UInt8>(offset & 0xFF));
    b.push_back(static_cast<UInt8>((offset >> 8) & 0xFF));
    if (ml >= 15)
        appendLengthExtension(b, ml - 15);

    const size_t tl = trailing.size();
    const UInt8 tl_nibble = static_cast<UInt8>(std::min<size_t>(tl, 15));
    b.push_back(static_cast<UInt8>(tl_nibble << 4));
    if (tl >= 15)
        appendLengthExtension(b, tl - 15);
    b.insert(b.end(), trailing.begin(), trailing.end());

    return b;
}

/// The bytes that decoding `encodeBlock(seed, offset, match_length, trailing)` must produce,
/// derived independently from the LZ4 overlap semantics.
std::vector<UInt8> expectedOutput(
    const std::vector<UInt8> & seed,
    UInt16 offset,
    size_t match_length,
    const std::vector<UInt8> & trailing)
{
    std::vector<UInt8> out = seed;
    for (size_t i = 0; i < match_length; ++i)
        out.push_back(out[out.size() - offset]);
    out.insert(out.end(), trailing.begin(), trailing.end());
    return out;
}

/// Decode through the forced branchless variant (method 1 = `decompressImpl<16, true>`).
/// Both buffers carry the mandatory `ADDITIONAL_BYTES_AT_END_OF_BUFFER` slack so the
/// wild copies never read or write out of bounds.
bool decodeWithForcedVariant(
    const std::vector<UInt8> & block,
    size_t dest_size,
    std::vector<UInt8> & out)
{
    std::vector<UInt8> source(block.size() + LZ4::ADDITIONAL_BYTES_AT_END_OF_BUFFER, 0);
    std::copy(block.begin(), block.end(), source.begin());

    out.assign(dest_size + LZ4::ADDITIONAL_BYTES_AT_END_OF_BUFFER, 0);

    LZ4::PerformanceStatistics stat{1}; /// Force method 1 regardless of block size.
    EXPECT_EQ(stat.choose_method, 1);

    const bool ok = LZ4::decompress(
        reinterpret_cast<const char *>(source.data()),
        reinterpret_cast<char *>(out.data()),
        block.size(),
        dest_size,
        stat);

    out.resize(dest_size);
    return ok;
}

const std::vector<size_t> match_lengths = {4, 15, 16, 17, 31, 32};

}

/// For every small offset and a set of match lengths straddling the 16-byte copy
/// granularity, the forced branchless variant must agree byte-for-byte with both an
/// independent reference computation and the canonical `LZ4_decompress_safe` decoder.
TEST(LZ4DecompressBranchless, ForcedMethod1MatchesReferenceForSmallOffsets)
{
    /// Non-trivial literal payloads so a wrong copy cannot accidentally pass.
    std::vector<UInt8> trailing(18);
    for (size_t i = 0; i < trailing.size(); ++i)
        trailing[i] = static_cast<UInt8>(0x80 + i);

    for (UInt16 offset = 1; offset <= 15; ++offset)
    {
        std::vector<UInt8> seed(offset);
        for (size_t i = 0; i < seed.size(); ++i)
            seed[i] = static_cast<UInt8>(0x10 + i);

        for (size_t match_length : match_lengths)
        {
            SCOPED_TRACE("offset=" + std::to_string(offset) + " match_length=" + std::to_string(match_length));

            const std::vector<UInt8> block = encodeBlock(seed, offset, match_length, trailing);
            const std::vector<UInt8> expected = expectedOutput(seed, offset, match_length, trailing);
            const size_t dest_size = expected.size();

            /// Forced branchless variant.
            std::vector<UInt8> got;
            ASSERT_TRUE(decodeWithForcedVariant(block, dest_size, got));
            ASSERT_EQ(got, expected);

            /// Canonical reference decoder.
            std::vector<UInt8> reference(dest_size);
            const int ref_size = LZ4_decompress_safe(
                reinterpret_cast<const char *>(block.data()),
                reinterpret_cast<char *>(reference.data()),
                static_cast<int>(block.size()),
                static_cast<int>(dest_size));
            ASSERT_EQ(ref_size, static_cast<int>(dest_size));
            ASSERT_EQ(reference, expected);

            /// The whole point: forced variant == reference decoder.
            ASSERT_EQ(got, reference);
        }
    }
}

/// A block that is truncated before the match offset must be rejected (return false),
/// not silently reported as a successful decode. The reference decoder rejects it too.
TEST(LZ4DecompressBranchless, ForcedMethod1RejectsTruncatedInput)
{
    std::vector<UInt8> trailing(18, 0x55);
    std::vector<UInt8> seed = {0x10, 0x11, 0x12};
    const UInt16 offset = 3;
    const size_t match_length = 32;

    const std::vector<UInt8> block = encodeBlock(seed, offset, match_length, trailing);
    const size_t dest_size = expectedOutput(seed, offset, match_length, trailing).size();

    /// Cut the block off in the middle of the 2-byte match offset, so the stream is
    /// unambiguously malformed: the decoder runs out of input while reading the match.
    /// Layout up to here: token (1) + literals (seed) + first offset byte (1).
    const size_t truncated = 1 + seed.size() + 1;
    ASSERT_LT(truncated, block.size());
    const std::vector<UInt8> truncated_block(block.begin(), block.begin() + truncated);

    /// The forced variant must signal failure, never claim a successful decode.
    std::vector<UInt8> got;
    ASSERT_FALSE(decodeWithForcedVariant(truncated_block, dest_size, got));

    /// The reference decoder cannot reconstruct the full intended output either.
    std::vector<UInt8> reference(dest_size);
    const int ref_size = LZ4_decompress_safe(
        reinterpret_cast<const char *>(truncated_block.data()),
        reinterpret_cast<char *>(reference.data()),
        static_cast<int>(truncated_block.size()),
        static_cast<int>(dest_size));
    ASSERT_LT(ref_size, static_cast<int>(dest_size));
}

/// A block whose match offset points before the start of the output must be rejected.
TEST(LZ4DecompressBranchless, ForcedMethod1RejectsOutOfRangeOffset)
{
    std::vector<UInt8> trailing(18, 0x66);
    std::vector<UInt8> seed = {0x10, 0x11, 0x12};

    /// Offset 100 is far larger than the 3 bytes of output produced so far.
    const UInt16 bad_offset = 100;
    const size_t match_length = 16;
    const std::vector<UInt8> block = encodeBlock(seed, bad_offset, match_length, trailing);

    /// dest_size as if the (impossible) match had been decoded.
    const size_t dest_size = seed.size() + match_length + trailing.size();

    std::vector<UInt8> got;
    ASSERT_FALSE(decodeWithForcedVariant(block, dest_size, got));

    std::vector<UInt8> reference(dest_size);
    const int ref_size = LZ4_decompress_safe(
        reinterpret_cast<const char *>(block.data()),
        reinterpret_cast<char *>(reference.data()),
        static_cast<int>(block.size()),
        static_cast<int>(dest_size));
    ASSERT_LT(ref_size, 0);
}

/// A zero match offset is invalid in LZ4 (minimum match distance is 1). With `offset == 0` the
/// match pointer equals `op`, which slips past the `match < output_begin` guard, so the decoder
/// must reject it explicitly rather than synthesize output from the destination and report success.
///
/// Note the reference `LZ4_decompress_safe` is lenient here: it does not reject `offset == 0`, it
/// copies `op` onto itself and reports success with garbage output. Our decoder is deliberately
/// stricter (fail-closed) — the contract this test pins down is that it never claims success.
TEST(LZ4DecompressBranchless, ForcedMethod1RejectsZeroOffset)
{
    std::vector<UInt8> trailing(18, 0x77);
    std::vector<UInt8> seed = {0x10, 0x11, 0x12};

    const UInt16 zero_offset = 0;
    const size_t match_length = 16;
    const std::vector<UInt8> block = encodeBlock(seed, zero_offset, match_length, trailing);

    /// dest_size as if the (invalid) match had been decoded.
    const size_t dest_size = seed.size() + match_length + trailing.size();

    std::vector<UInt8> got;
    ASSERT_FALSE(decodeWithForcedVariant(block, dest_size, got));
}
