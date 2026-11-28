#include <Compression/LZ4_decompress_faster.h>

#include <base/types.h>

#include <lz4.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

/** These tests pin down the literal and match bounds of `LZ4::decompressImpl` for every production variant.
  *
  * The decoder is given `ADDITIONAL_BYTES_AT_END_OF_BUFFER` bytes of slack after the compressed payload
  * for its unconditional wild copies. Those bytes are not part of the payload: in a server they are either
  * uninitialized heap or the next compressed block, so a block whose literal declares more bytes than the
  * payload carries has to be rejected instead of being "decoded" from the slack.
  *
  * `LZ4::decompress` sends every block smaller than 32 KiB to method 0 (`decompressImpl<8>`), so the
  * functional test `05055_lz4_literal_past_end_of_compressed_block` only ever exercises that variant.
  * Here the variant is forced with `LZ4::PerformanceStatistics{method}`, so methods 1 (`decompressImpl<16, true>`)
  * and 2 (`decompressImpl<32>`) are covered on the very same crafted blocks.
  */

namespace
{

/// A sentinel that never appears in the crafted payloads. The slack after the payload is filled with it, so
/// if a literal is ever sourced from the slack, the output is recognizably wrong rather than accidentally zero.
constexpr UInt8 slack_sentinel = 0xEE;

/// A canary placed after the output slack. The decoder's contract is that it never writes further than
/// `ADDITIONAL_BYTES_AT_END_OF_BUFFER` past `dest_size`, whatever its verdict on the block, so any change
/// to the canary is an out-of-bounds write that a plain (non-sanitized) build would otherwise not notice.
constexpr UInt8 output_canary = 0xCC;
constexpr size_t output_canary_size = 1024;

/// Decode `block` into `dest_size` bytes through the forced variant `method`. Both buffers carry the mandatory
/// `ADDITIONAL_BYTES_AT_END_OF_BUFFER` slack, exactly as `CompressedReadBufferBase` provides it.
bool decodeWithForcedVariant(size_t method, const std::vector<UInt8> & block, size_t dest_size, std::vector<UInt8> & out)
{
    std::vector<UInt8> source(block.size() + LZ4::ADDITIONAL_BYTES_AT_END_OF_BUFFER, slack_sentinel);
    std::copy(block.begin(), block.end(), source.begin());

    const size_t canary_begin = dest_size + LZ4::ADDITIONAL_BYTES_AT_END_OF_BUFFER;
    out.assign(canary_begin + output_canary_size, 0);
    std::fill(out.begin() + canary_begin, out.end(), output_canary);

    LZ4::PerformanceStatistics stat{static_cast<ssize_t>(method)};
    EXPECT_EQ(stat.choose_method, static_cast<ssize_t>(method));

    const bool ok = LZ4::decompress(
        reinterpret_cast<const char *>(source.data()),
        reinterpret_cast<char *>(out.data()),
        block.size(),
        dest_size,
        stat);

    EXPECT_TRUE(std::all_of(out.begin() + canary_begin, out.end(), [](UInt8 byte) { return byte == output_canary; }))
        << "the decoder wrote past the output slack";

    out.resize(dest_size);
    return ok;
}

/// The canonical reference decoder's verdict on the same block: the produced size, or a negative value on error.
int decodeWithReference(const std::vector<UInt8> & block, size_t dest_size)
{
    std::vector<UInt8> reference(dest_size);
    return LZ4_decompress_safe(
        reinterpret_cast<const char *>(block.data()),
        reinterpret_cast<char *>(reference.data()),
        static_cast<int>(block.size()),
        static_cast<int>(dest_size));
}

/// A raw LZ4 block that consists of a single literal sequence declaring `declared` bytes, of which only
/// `carried` are actually present in the payload (`carried == declared` makes it well-formed).
std::vector<UInt8> literalOnlyBlock(size_t declared, size_t carried)
{
    std::vector<UInt8> block;
    block.push_back(static_cast<UInt8>(std::min<size_t>(declared, 15) << 4));
    if (declared >= 15)
    {
        size_t remainder = declared - 15;
        while (remainder >= 255)
        {
            block.push_back(255);
            remainder -= 255;
        }
        block.push_back(static_cast<UInt8>(remainder));
    }
    block.insert(block.end(), carried, 0x41);
    return block;
}

/// The LZ4 format requires the last sequence of a block to be literals only (at least 5 of them) and the last
/// match to start at least 12 bytes before the end of the block, so the trailing literals make the reference
/// decoder accept the well-formed blocks for every match length from the minimum of 4 upwards. They are also
/// what makes a missing match-side `output_end` check observable: a match that overruns the output by a few
/// bytes lands inside the slack, and the literal-side check then compares against a negative distance and
/// passes, so it is this literal that would be written wholesale past the slack, into the canary.
constexpr size_t last_literals = 200;

/// A raw LZ4 block of two sequences: 4 literals (`0x41`), then a match with offset 1 that declares
/// `match_length` bytes (at least 4, the LZ4 minimum), then the final literal-only sequence of
/// `last_literals` bytes (`0x42`). A well-formed block decodes into `4 + match_length + last_literals` bytes;
/// a destination with less room for the match is what the match-side `output_end` check has to reject.
std::vector<UInt8> literalThenMatchBlock(size_t match_length)
{
    const size_t match_nibble = std::min<size_t>(match_length - 4, 15);

    std::vector<UInt8> block;
    block.push_back(static_cast<UInt8>((4 << 4) | match_nibble));
    block.insert(block.end(), 4, 0x41);
    /// Offset 1, little-endian.
    block.push_back(0x01);
    block.push_back(0x00);
    if (match_nibble == 15)
    {
        size_t remainder = match_length - 4 - 15;
        while (remainder >= 255)
        {
            block.push_back(255);
            remainder -= 255;
        }
        block.push_back(static_cast<UInt8>(remainder));
    }
    /// The final sequence: literals only.
    std::vector<UInt8> tail = literalOnlyBlock(last_literals, last_literals);
    std::replace(tail.begin(), tail.end(), static_cast<UInt8>(0x41), static_cast<UInt8>(0x42));
    block.insert(block.end(), tail.begin(), tail.end());
    return block;
}

class LZ4DecompressLiteralBounds : public ::testing::TestWithParam<size_t>
{
};

}

/// The same block as in the functional test: `size_decompressed` is 40 and the literal declares 40 bytes,
/// but only 8 are supplied. Before the fix the remaining 32 bytes were copied out of the slack and the decoder
/// reported success.
TEST_P(LZ4DecompressLiteralBounds, RejectsLiteralPastEndOfPayload)
{
    const std::vector<UInt8> block = literalOnlyBlock(40, 8);
    ASSERT_EQ(block.size(), 10u);

    std::vector<UInt8> got;
    ASSERT_FALSE(decodeWithForcedVariant(GetParam(), block, 40, got));
    ASSERT_LT(decodeWithReference(block, 40), 0);
}

/// The boundary case: the literal is short by a single byte.
TEST_P(LZ4DecompressLiteralBounds, RejectsLiteralOneBytePastEndOfPayload)
{
    const std::vector<UInt8> block = literalOnlyBlock(40, 39);

    std::vector<UInt8> got;
    ASSERT_FALSE(decodeWithForcedVariant(GetParam(), block, 40, got));
    ASSERT_LT(decodeWithReference(block, 40), 0);
}

/// A literal that ends exactly at the end of the payload is well-formed and has to keep decoding, with the
/// wild copy's overshoot into the slack never reaching the output. This is also the block that the functional
/// test uses to prime the slack.
TEST_P(LZ4DecompressLiteralBounds, AcceptsLiteralEndingExactlyAtEndOfPayload)
{
    for (size_t length : {1uz, 7uz, 8uz, 15uz, 16uz, 31uz, 32uz, 33uz, 40uz, 200uz, 300uz})
    {
        SCOPED_TRACE("length=" + std::to_string(length));

        const std::vector<UInt8> block = literalOnlyBlock(length, length);
        const std::vector<UInt8> expected(length, 0x41);

        std::vector<UInt8> got;
        ASSERT_TRUE(decodeWithForcedVariant(GetParam(), block, length, got));
        ASSERT_EQ(got, expected);
        ASSERT_EQ(decodeWithReference(block, length), static_cast<int>(length));
    }
}

/// A literal that is fully carried by the payload but does not fit into the declared decompressed size.
TEST_P(LZ4DecompressLiteralBounds, RejectsLiteralPastEndOfOutput)
{
    const std::vector<UInt8> block = literalOnlyBlock(40, 40);

    std::vector<UInt8> got;
    ASSERT_FALSE(decodeWithForcedVariant(GetParam(), block, 39, got));
    ASSERT_LT(decodeWithReference(block, 39), 0);
}

/// A literal length that is nowhere near the payload: the length-extension bytes declare about 4 KiB, so the
/// decoder has to reject the block by comparing distances instead of forming a pointer far outside the buffer.
TEST_P(LZ4DecompressLiteralBounds, RejectsHugeDeclaredLiteral)
{
    const std::vector<UInt8> block = literalOnlyBlock(4096, 8);

    std::vector<UInt8> got;
    ASSERT_FALSE(decodeWithForcedVariant(GetParam(), block, 40, got));
    ASSERT_FALSE(decodeWithForcedVariant(GetParam(), block, 4096, got));
    ASSERT_LT(decodeWithReference(block, 4096), 0);
}

/// A token that promises a length extension, with the payload ending right after the token. The decoder is
/// standing exactly at the end of the payload here and must not read the extension byte from the slack.
TEST_P(LZ4DecompressLiteralBounds, RejectsMissingLengthExtension)
{
    const std::vector<UInt8> block = {0xF0};

    std::vector<UInt8> got;
    ASSERT_FALSE(decodeWithForcedVariant(GetParam(), block, 40, got));
    ASSERT_LT(decodeWithReference(block, 40), 0);
}

/// A literal followed by a match whose two offset bytes are missing: the payload ends right after the literal,
/// so the offset would have to come from the slack. The reference decoder is lenient here and treats the block
/// as a legitimate final literal-only sequence of 4 bytes; what matters is that neither decoder claims to have
/// produced the 8 declared bytes.
TEST_P(LZ4DecompressLiteralBounds, RejectsMissingMatchOffset)
{
    /// Token: 4 literals, match length nibble 0 (a match of 4), then the 4 literals and nothing else.
    const std::vector<UInt8> block = {0x40, 0x41, 0x41, 0x41, 0x41};

    std::vector<UInt8> got;
    ASSERT_FALSE(decodeWithForcedVariant(GetParam(), block, 8, got));
    ASSERT_LT(decodeWithReference(block, 8), 8);
}

/// A well-formed match that fits exactly into what is left of the output before the trailing literals.
/// This is the positive control for the match-side `output_end` check: the match reaches the end of the
/// output minus `last_literals` bytes, and decoding has to continue into the final literals instead of
/// being rejected.
TEST_P(LZ4DecompressLiteralBounds, AcceptsMatchFittingIntoOutput)
{
    for (size_t match_length : {4uz, 5uz, 8uz, 15uz, 16uz, 18uz, 19uz, 31uz, 32uz, 33uz, 64uz, 65uz, 300uz})
    {
        SCOPED_TRACE("match_length=" + std::to_string(match_length));

        const std::vector<UInt8> block = literalThenMatchBlock(match_length);
        const size_t dest_size = 4 + match_length + last_literals;

        std::vector<UInt8> expected(4 + match_length, 0x41);
        expected.insert(expected.end(), last_literals, 0x42);

        std::vector<UInt8> got;
        ASSERT_TRUE(decodeWithForcedVariant(GetParam(), block, dest_size, got));
        ASSERT_EQ(got, expected);
        ASSERT_EQ(decodeWithReference(block, dest_size), static_cast<int>(dest_size));
    }
}

/// The match is fully described by the payload but declares more bytes than are left in the output: the
/// decoder has to reject it before `copyOverlap` / `wildCopyFromOutput` run past `output_end`. The boundary
/// case is a match that is a single byte too long, then a few bytes more, for every match length class the
/// variants treat differently. The excess stays below the match length, so the 4 leading literals always fit
/// without filling the output (which would be a legitimate end of decoding), and it is the match itself that
/// is rejected.
TEST_P(LZ4DecompressLiteralBounds, RejectsMatchPastEndOfOutput)
{
    for (size_t match_length : {4uz, 5uz, 8uz, 15uz, 16uz, 18uz, 19uz, 31uz, 32uz, 33uz, 64uz, 65uz, 300uz})
    {
        for (size_t excess : {1uz, 2uz, 3uz, 4uz, 7uz})
        {
            if (excess >= match_length)
                continue;

            SCOPED_TRACE("match_length=" + std::to_string(match_length) + " excess=" + std::to_string(excess));

            const std::vector<UInt8> block = literalThenMatchBlock(match_length);
            const size_t dest_size = 4 + match_length - excess;

            std::vector<UInt8> got;
            ASSERT_FALSE(decodeWithForcedVariant(GetParam(), block, dest_size, got));
            ASSERT_LT(decodeWithReference(block, dest_size), 0);
        }
    }
}

/// A match length that is nowhere near the output: the length-extension bytes declare about 4 KiB into a
/// 40-byte destination, so the decoder has to reject the block by comparing distances instead of forming a
/// pointer far outside the buffer.
TEST_P(LZ4DecompressLiteralBounds, RejectsHugeDeclaredMatch)
{
    const std::vector<UInt8> block = literalThenMatchBlock(4096);

    std::vector<UInt8> got;
    ASSERT_FALSE(decodeWithForcedVariant(GetParam(), block, 40, got));
    ASSERT_FALSE(decodeWithForcedVariant(GetParam(), block, 4096, got));
    ASSERT_LT(decodeWithReference(block, 40), 0);
    ASSERT_LT(decodeWithReference(block, 4096), 0);
}

/// A token that promises a match-length extension, with the payload ending right after the offset. The
/// decoder is standing exactly at the end of the payload here and must not read the extension byte from
/// the slack.
TEST_P(LZ4DecompressLiteralBounds, RejectsMissingMatchLengthExtension)
{
    /// Token: 4 literals, match length nibble 15; the 4 literals; offset 1; and nothing else.
    const std::vector<UInt8> block = {0x4F, 0x41, 0x41, 0x41, 0x41, 0x01, 0x00};

    std::vector<UInt8> got;
    ASSERT_FALSE(decodeWithForcedVariant(GetParam(), block, 40, got));
    ASSERT_LT(decodeWithReference(block, 40), 0);
}

INSTANTIATE_TEST_SUITE_P(
    AllVariants,
    LZ4DecompressLiteralBounds,
    ::testing::Values(0uz, 1uz, 2uz),
    [](const ::testing::TestParamInfo<size_t> & param_info) { return "Method" + std::to_string(param_info.param); });
