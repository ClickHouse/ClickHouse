#include <Compression/CompressionFactory.h>
#include <Compression/ICompressionCodec.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/IParser.h>
#include <Parsers/TokenIterator.h>
#include <Common/Exception.h>

#include <cstring>
#include <vector>

#include <gtest/gtest.h>

/** The `PCO` codec writes the per-column block `[1 byte: W][1 byte: B][B raw leading bytes][standalone .pco]`,
  * where `W` is the element width and `B = uncompressed_size mod W`. Its method byte `0x9d` is dispatched by the
  * shared `CompressedReadBuffer`, so a malformed block can reach `doDecompressData` from unchecked external
  * framed input (the HTTP `decompress=1` path). These tests pin the fail-closed header contract: a valid block
  * round-trips, but an unsupported `W`, a stored `B` that disagrees with the output size, or a partial-only body
  * with its trailing standalone stream stripped off are all rejected. The round-trip and reference-fixture tests
  * do not exercise this contract because they only ever feed the decoder well-formed blocks.
  */

using namespace DB;

namespace
{

/// The 9-byte block header (method byte, compressed size, uncompressed size) precedes the codec body,
/// so `W` is at offset 9 and `B` at offset 10 of the frame produced by `ICompressionCodec::compress`.
constexpr size_t HEADER_SIZE = 9;

CompressionCodecPtr makePcoCodec(const DataTypePtr & data_type)
{
    const std::string codec_statement = "(PCO)";
    Tokens tokens(codec_statement.data(), codec_statement.data() + codec_statement.size());
    IParser::Pos token_iterator(tokens, 0, 0);

    Expected expected;
    ASTPtr codec_ast;
    ParserCodec parser;
    parser.parse(token_iterator, codec_ast, expected);

    return CompressionCodecFactory::instance().get(codec_ast, data_type);
}

std::vector<char> compressBytes(const CompressionCodecPtr & codec, const char * source, UInt32 source_size)
{
    std::vector<char> encoded(codec->getCompressedReserveSize(source_size));
    const UInt32 encoded_size = codec->compress(source, source_size, encoded.data());
    encoded.resize(encoded_size);
    return encoded;
}

}

TEST(CodecPcoMalformedBlock, RejectsUnsupportedElementWidth)
{
    auto codec = makePcoCodec(std::make_shared<DataTypeUInt32>());
    const std::vector<UInt32> values{1, 2, 3, 100, 200, 65535, 0, 4294967295u};
    const auto encoded = compressBytes(codec, reinterpret_cast<const char *>(values.data()), static_cast<UInt32>(values.size() * sizeof(UInt32)));

    std::vector<char> decoded(values.size() * sizeof(UInt32));
    /// The untampered block round-trips.
    ASSERT_NO_THROW(codec->decompress(encoded.data(), static_cast<UInt32>(encoded.size()), decoded.data()));
    ASSERT_EQ(std::memcmp(decoded.data(), values.data(), decoded.size()), 0);

    /// The element width must be one of 1, 2, 4, 8; anything else is rejected.
    for (UInt8 bad_width : {UInt8{0}, UInt8{3}, UInt8{5}, UInt8{7}, UInt8{16}, UInt8{255}})
    {
        auto bad = encoded;
        bad[HEADER_SIZE] = static_cast<char>(bad_width);
        EXPECT_THROW(codec->decompress(bad.data(), static_cast<UInt32>(bad.size()), decoded.data()), Exception)
            << "element width " << static_cast<int>(bad_width) << " should be rejected";
    }
}

TEST(CodecPcoMalformedBlock, RejectsMismatchedLeadingByteCount)
{
    auto codec = makePcoCodec(std::make_shared<DataTypeUInt32>());
    /// 7 UInt32 values = 28 bytes; with W = 4 the genuine B = 28 mod 4 = 0.
    const std::vector<UInt32> values{10, 20, 30, 40, 50, 60, 70};
    const auto encoded = compressBytes(codec, reinterpret_cast<const char *>(values.data()), static_cast<UInt32>(values.size() * sizeof(UInt32)));

    std::vector<char> decoded(values.size() * sizeof(UInt32));
    ASSERT_NO_THROW(codec->decompress(encoded.data(), static_cast<UInt32>(encoded.size()), decoded.data()));
    ASSERT_EQ(encoded[HEADER_SIZE + 1], static_cast<char>(0)); /// genuine B

    /// Claim a non-zero leading-byte count that the output size does not imply: must be rejected,
    /// not silently honoured.
    for (UInt8 bad_b : {UInt8{1}, UInt8{2}, UInt8{3}})
    {
        auto bad = encoded;
        bad[HEADER_SIZE + 1] = static_cast<char>(bad_b);
        EXPECT_THROW(codec->decompress(bad.data(), static_cast<UInt32>(bad.size()), decoded.data()), Exception)
            << "stored leading-byte count " << static_cast<int>(bad_b) << " should be rejected";
    }
}

TEST(CodecPcoMalformedBlock, RejectsPartialOnlyBlockMissingStandaloneStream)
{
    auto codec = makePcoCodec(std::make_shared<DataTypeUInt64>());
    /// A 4-byte output is smaller than the 8-byte element, so the whole block is a single leading partial
    /// value (W = 8, B = 4) followed by a header-only standalone stream.
    const std::vector<char> raw{'\x01', '\x02', '\x03', '\x04'};
    const auto encoded = compressBytes(codec, raw.data(), static_cast<UInt32>(raw.size()));

    ASSERT_EQ(encoded[HEADER_SIZE], static_cast<char>(8)); /// W
    ASSERT_EQ(encoded[HEADER_SIZE + 1], static_cast<char>(4)); /// B

    std::vector<char> decoded(raw.size());
    /// The well-formed partial-only block (with its trailing standalone stream) round-trips.
    ASSERT_NO_THROW(codec->decompress(encoded.data(), static_cast<UInt32>(encoded.size()), decoded.data()));
    ASSERT_EQ(std::memcmp(decoded.data(), raw.data(), raw.size()), 0);

    /// Drop the trailing standalone stream, keeping only the block header, W, B, and the 4 raw bytes.
    /// An early return would accept this; the decoder must instead require a valid standalone stream.
    const std::vector<char> truncated(encoded.begin(), encoded.begin() + HEADER_SIZE + 2 + raw.size());
    EXPECT_THROW(codec->decompress(truncated.data(), static_cast<UInt32>(truncated.size()), decoded.data()), Exception);
}
