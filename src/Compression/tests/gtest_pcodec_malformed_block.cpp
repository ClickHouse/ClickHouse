#include <Compression/CompressionFactory.h>
#include <Compression/ICompressionCodec.h>
#include <Compression/Pcodec/StandaloneEncoder.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/IParser.h>
#include <Parsers/TokenIterator.h>
#include <Common/Exception.h>

#include <cstdint>
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

/// Builds a single-`UInt32`-value standalone `.pco` stream encoded in `IntMult` mode with the given
/// `base` and `primary` latents and a constant-zero secondary. The layout mirrors the real encoder's
/// output for a one-value chunk (single-bin primary with a full-width offset, constant secondary), so
/// a well-formed `base`/`primary` round-trips; a `base == 0` or a `primary * base` that overflows the
/// 32-bit latent is malformed and must be rejected by the decoder's fail-closed guard.
std::vector<uint8_t> buildIntMultStandaloneU32(uint32_t base_latent, uint32_t primary_latent)
{
    using namespace DB::Pcodec;
    constexpr Bitlen l_bits = 32;
    const auto type_byte = static_cast<uint8_t>(NumberTraits<uint32_t>::type_byte);

    /// Over-sized so the BitWriter's up-to-16-byte write-past-the-end slack stays in bounds.
    std::vector<uint8_t> buf(256, 0);
    BitWriter writer(buf.data(), buf.size());

    writer.writeAlignedBytes(MAGIC_HEADER.data(), MAGIC_HEADER.size());
    writer.writeU64(CURRENT_STANDALONE_VERSION, BITS_TO_ENCODE_STANDALONE_VERSION);
    writer.writeAlignedBytes(&type_byte, 1); // uniform type
    writeVarint(writer, 1); // n hint
    writer.finishByte();

    const uint8_t version_bytes[2] = {4, 1}; // wrapped format 4.1
    writer.writeAlignedBytes(version_bytes, 2);

    writer.writeAlignedBytes(&type_byte, 1); // chunk type
    writer.writeU64(0, BITS_TO_ENCODE_N_ENTRIES); // n - 1 == 0 (one value)

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

    writer.finishByte(); // page metadata: no delta moments, 4 zero-width ANS finals

    writer.writeU64(primary_latent, l_bits); // page body: the single primary offset
    writer.finishByte();

    const uint8_t term = MAGIC_TERMINATION_BYTE;
    writer.writeAlignedBytes(&term, 1);

    buf.resize(writer.byteSize());
    return buf;
}

/// Splices a standalone `.pco` stream into a real `PCO` frame for a one-`UInt32` column: the real
/// 9-byte block header (correct method byte and `decompressed_size == 4`) plus `[W = 4][B = 0]` are
/// reused from `valid_frame` and only the trailing standalone stream is replaced. This is exactly the
/// external `0x9d` frame a client can send through the shared `CompressedReadBuffer`.
std::vector<char> spliceStandalone(const std::vector<char> & valid_frame, const std::vector<uint8_t> & standalone)
{
    std::vector<char> frame(valid_frame.begin(), valid_frame.begin() + HEADER_SIZE + 2);
    frame.insert(frame.end(), reinterpret_cast<const char *>(standalone.data()),
                 reinterpret_cast<const char *>(standalone.data()) + standalone.size());
    return frame;
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

TEST(CodecPcoMalformedBlock, RejectsTrailingBytesAfterStandaloneStream)
{
    auto codec = makePcoCodec(std::make_shared<DataTypeUInt32>());
    const std::vector<UInt32> values{1, 2, 3, 100, 200, 65535, 0, 4294967295u};
    const auto encoded = compressBytes(codec, reinterpret_cast<const char *>(values.data()), static_cast<UInt32>(values.size() * sizeof(UInt32)));

    std::vector<char> decoded(values.size() * sizeof(UInt32));
    /// The untampered block round-trips.
    ASSERT_NO_THROW(codec->decompress(encoded.data(), static_cast<UInt32>(encoded.size()), decoded.data()));
    ASSERT_EQ(std::memcmp(decoded.data(), values.data(), decoded.size()), 0);

    /// The block body ends at the standalone stream's termination byte. Appending bytes after it makes
    /// the body non-canonical (`[W][B][raw][valid .pco][garbage]`); the decoder must reject it instead
    /// of stopping at the terminator and silently ignoring the trailing bytes — the wrapper's produced
    /// byte count still equals the expected size, so that check alone does not notice them.
    for (size_t extra : {size_t{1}, size_t{4}, size_t{17}})
    {
        auto bad = encoded;
        bad.insert(bad.end(), extra, static_cast<char>(0xAB));
        EXPECT_THROW(codec->decompress(bad.data(), static_cast<UInt32>(bad.size()), decoded.data()), Exception)
            << extra << " trailing byte(s) after the standalone stream should be rejected";
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

TEST(CodecPcoMalformedBlock, RejectsMalformedIntMultDecomposition)
{
    auto codec = makePcoCodec(std::make_shared<DataTypeUInt32>());
    /// One real value gives a valid frame `[header][W = 4][B = 0][valid standalone]` whose header
    /// (method byte and `decompressed_size == 4`) is reused for the spliced streams below.
    const UInt32 value = 123456789;
    const auto valid = compressBytes(codec, reinterpret_cast<const char *>(&value), sizeof(value));
    ASSERT_EQ(valid[HEADER_SIZE], static_cast<char>(4)); /// W
    ASSERT_EQ(valid[HEADER_SIZE + 1], static_cast<char>(0)); /// B

    UInt32 decoded = 0;

    /// Baseline: a hand-built *well-formed* IntMult stream (base = 3, primary = 2 -> value 6) spliced
    /// into the real header round-trips through `codec->decompress`. This proves the splice harness and
    /// the stream builder are sound, so the rejections below are attributable to the decomposition guard.
    {
        const auto frame = spliceStandalone(valid, buildIntMultStandaloneU32(/*base_latent=*/3, /*primary_latent=*/2));
        ASSERT_NO_THROW(codec->decompress(frame.data(), static_cast<UInt32>(frame.size()), reinterpret_cast<char *>(&decoded)));
        EXPECT_EQ(decoded, 6u);
    }

    /// `primary * base` overflowing the 32-bit latent fabricates a value by wrapping; the decoder must
    /// fail closed instead of returning it, even though the block reaches it via the shared frame reader.
    {
        const auto frame = spliceStandalone(valid, buildIntMultStandaloneU32(/*base_latent=*/0x10000, /*primary_latent=*/0x10000));
        EXPECT_THROW(codec->decompress(frame.data(), static_cast<UInt32>(frame.size()), reinterpret_cast<char *>(&decoded)), Exception);
    }

    /// `base == 0` is never a valid IntMult base.
    {
        const auto frame = spliceStandalone(valid, buildIntMultStandaloneU32(/*base_latent=*/0, /*primary_latent=*/5));
        EXPECT_THROW(codec->decompress(frame.data(), static_cast<UInt32>(frame.size()), reinterpret_cast<char *>(&decoded)), Exception);
    }
}
