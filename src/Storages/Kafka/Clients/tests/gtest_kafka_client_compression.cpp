#include <Storages/Kafka/Clients/Protocol/Compression.h>

#include "config.h"

#include <Storages/Kafka/Clients/Error.h>

#include <gtest/gtest.h>

#include <base/types.h>

#if USE_SNAPPY
#    include <snappy.h>
#endif

#include <cstring>
#include <string_view>
#include <vector>

using Kafka::Bytes;
using Kafka::ErrorKind;
using Kafka::Protocol::compress;
using Kafka::Protocol::compressionSupported;
using Kafka::Protocol::CompressionType;
using Kafka::Protocol::compressionTypeName;
using Kafka::Protocol::decompress;

namespace
{

std::vector<String> testPayloads()
{
    /// 1 MiB of a repeating 0..255 pattern — spans several codec blocks.
    String patterned(1 << 20, '\0');
    for (size_t i = 0; i < patterned.size(); ++i)
        patterned[i] = static_cast<char>(i % 256);

    return {
        String(),
        String("hello"),
        std::move(patterned),
        String("with\0zero\0bytes", 15),
    };
}

void expectRoundtrip(CompressionType type, int level = -1)
{
    for (const auto & payload : testPayloads())
    {
        const Bytes compressed = compress(type, payload, level);
        /// EXPECT_TRUE instead of EXPECT_EQ: a failure must not dump a 1 MiB blob.
        EXPECT_TRUE(decompress(type, compressed) == payload)
            << compressionTypeName(type) << " roundtrip broke a payload of " << payload.size() << " bytes";
    }
}

void expectProtocolError(CompressionType type, std::string_view corrupt)
{
    try
    {
        decompress(type, corrupt);
        FAIL() << "Expected Kafka::Exception for corrupt " << compressionTypeName(type) << " input";
    }
    catch (const Kafka::Exception & e)
    {
        EXPECT_EQ(e.getKind(), ErrorKind::Protocol) << e.message();
    }
}

}

TEST(KafkaClientCompression, NonePassesThrough)
{
    for (const auto & payload : testPayloads())
    {
        EXPECT_TRUE(compress(CompressionType::None, payload) == payload);
        EXPECT_TRUE(decompress(CompressionType::None, payload) == payload);
    }
}

TEST(KafkaClientCompression, GzipRoundtrip)
{
    expectRoundtrip(CompressionType::Gzip);
}

TEST(KafkaClientCompression, GzipRoundtripExplicitLevel)
{
    expectRoundtrip(CompressionType::Gzip, 1);
}

TEST(KafkaClientCompression, GzipEmitsGzipWrapper)
{
    /// Kafka interop needs the gzip wrapper, not a raw zlib stream.
    const Bytes compressed = compress(CompressionType::Gzip, "hello");
    ASSERT_GE(compressed.size(), 2u);
    EXPECT_EQ(static_cast<UInt8>(compressed[0]), 0x1F);
    EXPECT_EQ(static_cast<UInt8>(compressed[1]), 0x8B);
}

TEST(KafkaClientCompression, Lz4Roundtrip)
{
    expectRoundtrip(CompressionType::Lz4);
}

TEST(KafkaClientCompression, ZstdRoundtrip)
{
    expectRoundtrip(CompressionType::Zstd);
}

TEST(KafkaClientCompression, ZstdRoundtripExplicitLevel)
{
    expectRoundtrip(CompressionType::Zstd, 5);
}

TEST(KafkaClientCompression, CorruptInputThrowsProtocolError)
{
    const String corrupt = "garbage that is not a valid compressed stream";
    expectProtocolError(CompressionType::Gzip, corrupt);
    expectProtocolError(CompressionType::Lz4, corrupt);
    expectProtocolError(CompressionType::Zstd, corrupt);
#if USE_SNAPPY
    expectProtocolError(CompressionType::Snappy, corrupt);
#endif
}

TEST(KafkaClientCompression, SupportedCodecs)
{
    EXPECT_TRUE(compressionSupported(CompressionType::None));
    EXPECT_TRUE(compressionSupported(CompressionType::Gzip));
    EXPECT_TRUE(compressionSupported(CompressionType::Lz4));
    EXPECT_TRUE(compressionSupported(CompressionType::Zstd));
#if USE_SNAPPY
    EXPECT_TRUE(compressionSupported(CompressionType::Snappy));
#else
    EXPECT_FALSE(compressionSupported(CompressionType::Snappy));
#endif
}

#if USE_SNAPPY

TEST(KafkaClientCompression, SnappyRoundtrip)
{
    expectRoundtrip(CompressionType::Snappy);
}

TEST(KafkaClientCompression, SnappyEmitsXerialFraming)
{
    /// Java's SnappyInputStream rejects anything without this header.
    const char expected_magic[8] = {'\x82', 'S', 'N', 'A', 'P', 'P', 'Y', '\0'};
    const Bytes compressed = compress(CompressionType::Snappy, "hello");
    ASSERT_GE(compressed.size(), sizeof(expected_magic));
    EXPECT_EQ(memcmp(compressed.data(), expected_magic, sizeof(expected_magic)), 0);
}

TEST(KafkaClientCompression, SnappyDecodesRawBlocks)
{
    /// librdkafka sends one bare snappy block without the xerial framing.
    const String payload = "raw snappy block produced by librdkafka";
    String raw_block;
    snappy::Compress(payload.data(), payload.size(), &raw_block);
    EXPECT_TRUE(decompress(CompressionType::Snappy, raw_block) == payload);
}

#endif
