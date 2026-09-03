#include <Compression/LZ4_decompress_faster.h>

#include <base/types.h>

#include <lz4.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

/** These tests pin down the literal bounds of `LZ4::decompressImpl` for every production variant.
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

/// Decode `block` into `dest_size` bytes through the forced variant `method`. Both buffers carry the mandatory
/// `ADDITIONAL_BYTES_AT_END_OF_BUFFER` slack, exactly as `CompressedReadBufferBase` provides it.
bool decodeWithForcedVariant(size_t method, const std::vector<UInt8> & block, size_t dest_size, std::vector<UInt8> & out)
{
    std::vector<UInt8> source(block.size() + LZ4::ADDITIONAL_BYTES_AT_END_OF_BUFFER, slack_sentinel);
    std::copy(block.begin(), block.end(), source.begin());

    out.assign(dest_size + LZ4::ADDITIONAL_BYTES_AT_END_OF_BUFFER, 0);

    LZ4::PerformanceStatistics stat{static_cast<ssize_t>(method)};
    EXPECT_EQ(stat.choose_method, static_cast<ssize_t>(method));

    const bool ok = LZ4::decompress(
        reinterpret_cast<const char *>(source.data()),
        reinterpret_cast<char *>(out.data()),
        block.size(),
        dest_size,
        stat);

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

INSTANTIATE_TEST_SUITE_P(
    AllVariants,
    LZ4DecompressLiteralBounds,
    ::testing::Values(0uz, 1uz, 2uz),
    [](const ::testing::TestParamInfo<size_t> & param_info) { return "Method" + std::to_string(param_info.param); });
