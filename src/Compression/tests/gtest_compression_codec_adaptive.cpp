#include <atomic>
#include <cstring>
#include <thread>
#include <vector>
#include <Compression/CompressionCodecAdaptive.h>
#include <Compression/CompressionCodecMultiple.h>
#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <Compression/ICompressionCodec.h>
#include <Core/Defines.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeFactory.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/IAST.h>
#include <Parsers/parseQuery.h>
#include <base/defines.h>
#include <base/unaligned.h>
#include <gtest/gtest.h>
#include <Common/PODArray.h>

using namespace DB;

namespace
{

constexpr auto T64 = static_cast<uint8_t>(CompressionMethodByte::T64);
constexpr auto NONE = static_cast<uint8_t>(CompressionMethodByte::NONE);

DataTypePtr type(const String & name)
{
    return DataTypeFactory::instance().get(name);
}

CompressionCodecPtr defaultCodec()
{
    return CompressionCodecFactory::instance().getDefaultCodec();
}

/// Serialize values to a little-endian byte buffer, the way the codecs read them back.
template <typename T>
std::vector<char> bytesOf(const std::vector<T> & values)
{
    std::vector<char> bytes(values.size() * sizeof(T));
    char * pos = bytes.data();
    for (const T value : values)
    {
        unalignedStoreLittleEndian<T>(pos, value);
        pos += sizeof(T);
    }
    return bytes;
}

/// Asserts order of codecs in the pool: [0] NONE, [1] the default, then `extras` in order.
void expectPool(const char * name, std::initializer_list<std::string_view> extras)
{
    Codecs pool;
    ASSERT_NO_THROW(pool = AdaptiveCodec::poolForType(type(name), defaultCodec())) << "type " << name;
    ASSERT_EQ(pool.size(), 2 + extras.size()) << "type " << name;
    EXPECT_EQ(pool[0]->getMethodByte(), NONE) << "type " << name; /// NONE is always [0]
    EXPECT_EQ(pool[1].get(), defaultCodec().get()) << "type " << name; /// default is always [1]
    size_t i = 2;
    for (const auto extra : extras)
        EXPECT_EQ(pool[i++]->getCodecDesc()->formatForLogging(), extra) << "type " << name;
}

/// Compress `bytes` with the adaptive codec for `type_name` and return the winner's on-disk method byte.
/// Round-trips the compressed block through the method-byte codec, as a normal reader would do.
uint8_t adaptiveWinnerByte(const String & type_name, const std::vector<char> & bytes)
{
    const UInt32 size = static_cast<UInt32>(bytes.size());
    CompressionCodecAdaptive adaptive(type(type_name), defaultCodec());

    PODArray<char> encoded(adaptive.getCompressedReserveSize(size));
    const UInt32 encoded_size = adaptive.compress(bytes.data(), size, encoded.data());

    const uint8_t method = ICompressionCodec::readMethod(encoded.data());
    auto decoder = CompressionCodecFactory::instance().get(method);

    /// Fast decompressors read and write in wide blocks past the logical end of both buffers.
    encoded.resize(encoded_size + decoder->getAdditionalSizeAtTheEndOfBuffer());
    PODArray<char> decoded(size + decoder->getAdditionalSizeAtTheEndOfBuffer());
    EXPECT_EQ(decoder->decompress(encoded.data(), encoded_size, decoded.data()), size);
    EXPECT_EQ(0, memcmp(decoded.data(), bytes.data(), size));
    return method;
}

}

TEST(AdaptiveCodecPool, CandidateTypesGetT64)
{
    for (const auto * name :
         {"Int8",
          "Int16",
          "Int32",
          "Int64",
          "UInt8",
          "UInt16",
          "UInt32",
          "UInt64",
          "Enum8('a' = 1)",
          "Enum16('a' = 1)",
          "Date",
          "Date32",
          "DateTime",
          "DateTime64(3)",
          "Time",
          "Time64(3)",
          "Decimal(9, 2)",
          "Decimal(18, 2)",
          "IPv4"})
        expectPool(name, {"T64"});
}

TEST(AdaptiveCodecPool, FloatTypesGetALPVariants)
{
    /// STD before RD because STD decompressed faster (we want it in case of tie)
    for (const auto * name : {"Float32", "Float64"})
        expectPool(name, {"ALP(STD)", "ALP(RD)"});
}

TEST(AdaptiveCodecPool, NonCandidateTypesGetNoneAndDefaultOnly)
{
    for (const auto * name : {"Int128", "UInt256", "Decimal(38, 2)", "String", "UUID", "BFloat16"})
        expectPool(name, {});
}

TEST(AdaptiveCodecPool, MultipleCodecAggregatesEncryption)
{
    auto & factory = CompressionCodecFactory::instance();
    EXPECT_TRUE(factory.get("AES_128_GCM_SIV")->isEncryption());
    EXPECT_TRUE(factory.get("LZ4, AES_128_GCM_SIV")->isEncryption());
    EXPECT_FALSE(factory.get("LZ4")->isEncryption());
    EXPECT_FALSE(factory.get("LZ4, ZSTD")->isEncryption());
}

TEST(CompressionCodecFactory, IsDefaultCodec)
{
    const auto codec = [](const String & expr)
    {
        static const Settings settings;
        ParserCodec parser;
        const String query = "(" + expr + ")";
        ASTPtr parsed = parseQuery(parser, query, /*max_query_size=*/0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
        return CompressionCodecFactory::instance().validateCodecAndGetPreprocessedAST(
            parsed, /*column_type=*/nullptr, CodecValidationSettings(settings));
    };

    /// No CODEC clause resolves to the default codec.
    EXPECT_TRUE(CompressionCodecFactory::isDefaultCodec(nullptr));
    EXPECT_TRUE(CompressionCodecFactory::isDefaultCodec(codec("Default")));

    /// Anything else is an explicit user choice, even a chain containing Default.
    EXPECT_FALSE(CompressionCodecFactory::isDefaultCodec(codec("LZ4")));
    EXPECT_FALSE(CompressionCodecFactory::isDefaultCodec(codec("ZSTD(3)")));
    EXPECT_FALSE(CompressionCodecFactory::isDefaultCodec(codec("Delta, Default")));
}

TEST(CompressionCodecAdaptive, MonotonicNarrowIntegersPickT64)
{
    std::vector<UInt32> values(100000);
    for (size_t i = 0; i < values.size(); ++i)
        values[i] = static_cast<UInt32>(i);

    EXPECT_EQ(adaptiveWinnerByte("UInt32", bytesOf(values)), T64);
}

TEST(CompressionCodecAdaptive, RepeatingWideValuesPickDefault)
{
    /// A short pattern of full-range values, repeated: a general-purpose codec crushes the repetition,
    /// but T64 sees a wide min/max and cannot shrink it. The winner must be the default anchor, whatever
    /// the built-in default codec is (`ZSTD(3)` today).
    const std::vector<UInt32> pattern = {0u, 0xFFFFFFFFu, 0x0F0F0F0Fu, 0xF0F0F0F0u, 0x12345678u, 0x9ABCDEF0u, 0xDEADBEEFu, 0xCAFEBABEu};
    std::vector<UInt32> values(100000);
    for (size_t i = 0; i < values.size(); ++i)
        values[i] = pattern[i % pattern.size()];

    EXPECT_EQ(adaptiveWinnerByte("UInt32", bytesOf(values)), defaultCodec()->getMethodByte());
}

TEST(CompressionCodecAdaptive, ConstantColumnPicksT64)
{
    std::vector<UInt32> values(100000, 42u);

    EXPECT_EQ(adaptiveWinnerByte("UInt32", bytesOf(values)), T64);
}

TEST(CompressionCodecAdaptive, TinyBlockStoredRaw)
{
    /// 16 bytes: every compressing candidate's framing alone exceeds the raw bytes, so NONE wins.
    std::vector<UInt32> values(4);
    for (size_t i = 0; i < values.size(); ++i)
        values[i] = static_cast<UInt32>(i);

    EXPECT_EQ(adaptiveWinnerByte("UInt32", bytesOf(values)), NONE);
}

TEST(CompressionCodecAdaptive, HashHasOwnNamespace)
{
    /// getHash() identifies the codec when the Compact writer groups column streams. CompressionCodecAdaptive and CompressionCodecMultiple
    /// fold their children's hashes the same way, so the leading "Adaptive" descriptor is what keeps the two distinct.
    CompressionCodecAdaptive adaptive(type("UInt32"), defaultCodec());
    auto pool = AdaptiveCodec::poolForType(type("UInt32"), defaultCodec());
    ASSERT_EQ(pool.size(), 3u);
    CompressionCodecMultiple multiple(pool);
    EXPECT_NE(adaptive.getHash(), multiple.getHash());

    CompressionCodecAdaptive adaptive_string(type("String"), defaultCodec());
    EXPECT_NE(adaptive_string.getHash(), defaultCodec()->getHash());

    CompressionCodecAdaptive adaptive_int64(type("Int64"), defaultCodec());
    EXPECT_NE(adaptive.getHash(), adaptive_int64.getHash());
}

TEST(CompressionCodecAdaptive, DirectInvocationThrows)
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    GTEST_SKIP() << "this test triggers LOGICAL_ERROR, runs only if DEBUG_OR_SANITIZER_BUILD is not defined";
#else
    CompressionCodecAdaptive adaptive(type("UInt32"), defaultCodec());
    /// Adaptive never appears on disk, so the public method byte accessor must reject direct use.
    EXPECT_ANY_THROW(adaptive.getMethodByte());
#endif
}

TEST(CompressionCodecAdaptive, ConcurrentCompressIsThreadSafe)
{
    CompressionCodecAdaptive adaptive(type("UInt32"), defaultCodec());

    constexpr size_t num_threads = 8;
    std::vector<std::thread> threads;
    std::atomic<bool> ok{true};
    for (size_t t = 0; t < num_threads; ++t)
    {
        threads.emplace_back(
            [&adaptive, t, &ok]()
            {
                std::vector<UInt32> values(10000);
                for (size_t i = 0; i < values.size(); ++i)
                    values[i] = static_cast<UInt32>(i + t); /// per-thread distinct buffer
                auto bytes = bytesOf(values);
                const UInt32 size = static_cast<UInt32>(bytes.size());

                PODArray<char> encoded(adaptive.getCompressedReserveSize(size));
                const UInt32 encoded_size = adaptive.compress(bytes.data(), size, encoded.data());

                auto decoder = CompressionCodecFactory::instance().get(ICompressionCodec::readMethod(encoded.data()));
                PODArray<char> decoded(size);
                if (decoder->decompress(encoded.data(), encoded_size, decoded.data()) != size
                    || memcmp(decoded.data(), bytes.data(), size) != 0)
                    ok = false;
            });
    }
    for (auto & th : threads)
        th.join();
    EXPECT_TRUE(ok);
}

TEST(TryGetCompressedSize, MatchesCompressForT64)
{
    std::vector<UInt32> values(50000);
    for (size_t i = 0; i < values.size(); ++i)
        values[i] = static_cast<UInt32>(i);
    auto bytes = bytesOf(values);
    const UInt32 size = static_cast<UInt32>(bytes.size());

    auto pool = AdaptiveCodec::poolForType(type("UInt32"), defaultCodec());
    ASSERT_EQ(pool.size(), 3u);
    const auto & t64 = pool[2];
    ASSERT_EQ(t64->getMethodByte(), T64);

    const auto calculated = t64->tryGetCompressedSize(bytes.data(), size);
    ASSERT_TRUE(calculated.has_value());

    /// Re-derive size from a real compress.
    PODArray<char> encoded(t64->getCompressedReserveSize(size));
    const UInt32 actual = t64->compress(bytes.data(), size, encoded.data());
    EXPECT_EQ(ICompressionCodec::getHeaderSize() + *calculated, actual);
}
