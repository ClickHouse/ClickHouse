#include <Storages/CompressionCodecSelector.h>
#include <Compression/CompressionFactory.h>
#include <Compression/ICompressionCodec.h>

#include <gtest/gtest.h>

/// The built-in default of `CompressionCodecSelector` (used when no `<compression>` `<case>` matched,
/// including when there is no `<compression>` configuration at all) is size-aware: parts smaller than
/// 100 MiB use the faster `LZ4`, while larger parts use the default codec (`ZSTD(3)`). This keeps
/// compression cheap for small, frequently rewritten parts (freshly inserted parts are passed a size
/// of `0`, so they start as `LZ4`) while getting the better ratio for the bulk of the data, which
/// lives in the large parts produced by background merges.

using namespace DB;

namespace
{
/// Must match `CompressionCodecSelector::min_part_size_for_default_codec`.
constexpr size_t threshold = 100 * 1024 * 1024;
}

TEST(CompressionCodecSelector, SmallPartsUseLZ4ByDefault)
{
    const auto & factory = CompressionCodecFactory::instance();
    const uint8_t lz4_byte = factory.get("LZ4", {})->getMethodByte();

    CompressionCodecSelector selector; /// No configuration: pure built-in default.

    /// A freshly inserted part is passed a size of `0` (its final size is not yet known).
    EXPECT_EQ(selector.choose(0, 0.0)->getMethodByte(), lz4_byte);
    EXPECT_EQ(selector.choose(1, 0.0)->getMethodByte(), lz4_byte);
    EXPECT_EQ(selector.choose(50 * 1024 * 1024, 0.0)->getMethodByte(), lz4_byte);
    /// Just below the threshold is still `LZ4`.
    EXPECT_EQ(selector.choose(threshold - 1, 0.0)->getMethodByte(), lz4_byte);
}

TEST(CompressionCodecSelector, LargePartsUseDefaultCodec)
{
    const auto & factory = CompressionCodecFactory::instance();
    const auto default_codec = factory.getDefaultCodec(); /// `ZSTD(3)`.

    CompressionCodecSelector selector; /// No configuration: pure built-in default.

    /// At and above the threshold the default codec (`ZSTD(3)`) is used.
    EXPECT_EQ(selector.choose(threshold, 0.0)->getMethodByte(), default_codec->getMethodByte());
    EXPECT_EQ(selector.choose(200 * 1024 * 1024, 0.0)->getMethodByte(), default_codec->getMethodByte());

    /// It is exactly the shared default codec instance, so this pins the level (`ZSTD(3)`), not just the family.
    EXPECT_EQ(selector.choose(threshold, 0.0), default_codec);
    EXPECT_EQ(selector.choose(200 * 1024 * 1024, 0.0), default_codec);

    /// The default codec is `ZSTD`, not `LZ4`, so the size split is actually observable.
    EXPECT_NE(default_codec->getMethodByte(), factory.get("LZ4", {})->getMethodByte());
}
