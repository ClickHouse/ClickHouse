#include <gtest/gtest.h>

#include <Compression/CompressedSizeEstimator.h>
#include <Compression/CompressionCodecAdaptive.h>
#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <Compression/ICompressionCodec.h>
#include <Core/TypeId.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <base/unaligned.h>
#include <Common/PODArray.h>

#include <atomic>
#include <cstring>
#include <thread>
#include <vector>

using namespace DB;

namespace
{

constexpr auto LZ4 = static_cast<uint8_t>(CompressionMethodByte::LZ4);
constexpr auto T64 = static_cast<uint8_t>(CompressionMethodByte::T64);

DataTypePtr type(const String & name)
{
    return DataTypeFactory::instance().get(name);
}

CompressionCodecPtr defaultCodec()
{
    return CompressionCodecFactory::instance().getDefaultCodec();
}

/// A constructible representative IDataType per scalar (codec-leaf) TypeIndex. Deliberately covers more than today's candidates.
/// If someone makes one of these a candidate for a codec that rejects it, the drift guard builds it and the rejection is caught at once.
/// Composite and wrapper types (Array, Tuple, Map, Nullable, ...) are never codec leaves, so they return nullptr.
DataTypePtr representativeType(TypeIndex idx)
{
    switch (idx)
    {
        case TypeIndex::Int8: return type("Int8");
        case TypeIndex::Int16: return type("Int16");
        case TypeIndex::Int32: return type("Int32");
        case TypeIndex::Int64: return type("Int64");
        case TypeIndex::Int128: return type("Int128");
        case TypeIndex::Int256: return type("Int256");
        case TypeIndex::UInt8: return type("UInt8");
        case TypeIndex::UInt16: return type("UInt16");
        case TypeIndex::UInt32: return type("UInt32");
        case TypeIndex::UInt64: return type("UInt64");
        case TypeIndex::UInt128: return type("UInt128");
        case TypeIndex::UInt256: return type("UInt256");
        case TypeIndex::BFloat16: return type("BFloat16");
        case TypeIndex::Float32: return type("Float32");
        case TypeIndex::Float64: return type("Float64");
        case TypeIndex::Decimal32: return type("Decimal(9, 2)");
        case TypeIndex::Decimal64: return type("Decimal(18, 2)");
        case TypeIndex::Decimal128: return type("Decimal(38, 2)");
        case TypeIndex::Decimal256: return type("Decimal(76, 2)");
        case TypeIndex::Date: return type("Date");
        case TypeIndex::Date32: return type("Date32");
        case TypeIndex::DateTime: return type("DateTime");
        case TypeIndex::DateTime64: return type("DateTime64(3)");
        case TypeIndex::Time: return type("Time");
        case TypeIndex::Time64: return type("Time64(3)");
        case TypeIndex::Enum8: return type("Enum8('a' = 1)");
        case TypeIndex::Enum16: return type("Enum16('a' = 1)");
        case TypeIndex::String: return type("String");
        case TypeIndex::FixedString: return type("FixedString(16)");
        case TypeIndex::UUID: return type("UUID");
        case TypeIndex::IPv4: return type("IPv4");
        case TypeIndex::IPv6: return type("IPv6");
        default: return nullptr;
    }
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

}

TEST(AdaptiveCodecPool, EachCandidateTypeBuildsWithDefaultAnchor)
{
    /// Drift guard. Every candidate must build for its type (a row naming a codec the type cannot take would otherwise throw at write time).
    for (const TypeIndex idx : AdaptiveCodec::candidateTypeIndexes())
    {
        const DataTypePtr t = representativeType(idx);
        ASSERT_NE(t, nullptr) << "no representative type for candidate TypeIndex " << static_cast<int>(idx)
                              << " -- add a case to representativeType()";

        std::vector<CompressionCodecPtr> pool;
        ASSERT_NO_THROW(pool = AdaptiveCodec::poolForType(*t, defaultCodec())) << "TypeIndex " << static_cast<int>(idx);
        EXPECT_EQ(pool[0].get(), defaultCodec().get()) << "TypeIndex " << static_cast<int>(idx); /// default is the anchor
        EXPECT_GE(pool.size(), 2u) << "TypeIndex " << static_cast<int>(idx); /// a candidate beyond the default was added
    }
}

TEST(AdaptiveCodecPool, RepresentativeTypesMatchTheirIndex)
{
    for (int i = 0; i < 256; ++i)
    {
        const auto idx = static_cast<TypeIndex>(i);
        if (const DataTypePtr t = representativeType(idx))
            EXPECT_EQ(t->getTypeId(), idx) << "representativeType built the wrong type for TypeIndex " << i;
    }
}

TEST(AdaptiveCodecPool, NonIntegerTypesGetDefaultOnly)
{
    for (const auto * name : {"Int128", "UInt256", "Decimal(38, 2)", "String", "Float32", "Float64", "UUID"})
    {
        auto pool = AdaptiveCodec::poolForType(*type(name), defaultCodec());
        EXPECT_EQ(pool.size(), 1u) << "type " << name;
        EXPECT_EQ(pool[0].get(), defaultCodec().get()) << "type " << name;
    }
}

TEST(AdaptiveCodecSelector, MonotonicNarrowIntegersPickT64)
{
    std::vector<UInt32> values(100000);
    for (size_t i = 0; i < values.size(); ++i)
        values[i] = static_cast<UInt32>(i);
    auto bytes = bytesOf(values);

    auto pool = AdaptiveCodec::poolForType(*type("UInt32"), defaultCodec());
    auto winner = AdaptiveCodec::select(pool, bytes.data(), static_cast<UInt32>(bytes.size()));
    EXPECT_EQ(winner->getMethodByte(), T64);
}

TEST(AdaptiveCodecSelector, RepeatingWideValuesPickDefault)
{
    /// A short pattern of full-range values, repeated: LZ4 crushes the repetition, but T64 sees a wide min/max cannot shrink it.
    const std::vector<UInt32> pattern = {0u, 0xFFFFFFFFu, 0x0F0F0F0Fu, 0xF0F0F0F0u, 0x12345678u, 0x9ABCDEF0u, 0xDEADBEEFu, 0xCAFEBABEu};
    std::vector<UInt32> values(100000);
    for (size_t i = 0; i < values.size(); ++i)
        values[i] = pattern[i % pattern.size()];
    auto bytes = bytesOf(values);

    auto pool = AdaptiveCodec::poolForType(*type("UInt32"), defaultCodec());
    auto winner = AdaptiveCodec::select(pool, bytes.data(), static_cast<UInt32>(bytes.size()));
    EXPECT_EQ(winner->getMethodByte(), LZ4);
}

TEST(AdaptiveCodecSelector, ConstantColumnPicksT64)
{
    std::vector<UInt32> values(100000, 42u);
    auto bytes = bytesOf(values);

    auto pool = AdaptiveCodec::poolForType(*type("UInt32"), defaultCodec());
    auto winner = AdaptiveCodec::select(pool, bytes.data(), static_cast<UInt32>(bytes.size()));
    EXPECT_EQ(winner->getMethodByte(), T64);
}

TEST(AdaptiveCodecSelector, TieGoesToEarliestCandidate)
{
    /// Two distinct but identical codecs produce the same size, the earliest must win (strict <).
    auto lz4a = CompressionCodecFactory::instance().get("LZ4", {});
    auto lz4b = CompressionCodecFactory::instance().get("LZ4", {});
    ASSERT_NE(lz4a.get(), lz4b.get());
    std::vector<CompressionCodecPtr> pool = {lz4a, lz4b};

    std::vector<UInt32> values(1000);
    for (size_t i = 0; i < values.size(); ++i)
        values[i] = static_cast<UInt32>(i * 7);
    auto bytes = bytesOf(values);

    auto winner = AdaptiveCodec::select(pool, bytes.data(), static_cast<UInt32>(bytes.size()));
    EXPECT_EQ(winner.get(), lz4a.get());
    EXPECT_NE(winner.get(), lz4b.get());
}

TEST(CompressionCodecAdaptive, CompressRoundTripsViaWinnerByte)
{
    std::vector<UInt32> values(100000);
    for (size_t i = 0; i < values.size(); ++i)
        values[i] = static_cast<UInt32>(i);
    auto bytes = bytesOf(values);
    const UInt32 size = static_cast<UInt32>(bytes.size());

    CompressionCodecAdaptive adaptive(*type("UInt32"), defaultCodec(), /*skip_threshold=*/0);

    PODArray<char> encoded(adaptive.getCompressedReserveSize(size));
    const UInt32 encoded_size = adaptive.compress(bytes.data(), size, encoded.data());

    const uint8_t method = ICompressionCodec::readMethod(encoded.data());
    EXPECT_EQ(method, T64); /// monotonic integers -> T64 wins

    /// Decode exactly as a normal reader would: look the codec up by the on-disk method byte.
    auto decoder = CompressionCodecFactory::instance().get(method);
    PODArray<char> decoded(size);
    const UInt32 decoded_size = decoder->decompress(encoded.data(), encoded_size, decoded.data());
    ASSERT_EQ(decoded_size, size);
    EXPECT_EQ(0, memcmp(decoded.data(), bytes.data(), size));
}

TEST(CompressionCodecAdaptive, BelowThresholdUsesDefault)
{
    std::vector<UInt32> values(4);
    for (size_t i = 0; i < values.size(); ++i)
        values[i] = static_cast<UInt32>(i);
    auto bytes = bytesOf(values);
    const UInt32 size = static_cast<UInt32>(bytes.size());

    CompressionCodecAdaptive adaptive(*type("UInt32"), defaultCodec(), /*skip_threshold=*/4096);

    PODArray<char> encoded(adaptive.getCompressedReserveSize(size));
    adaptive.compress(bytes.data(), size, encoded.data());
    EXPECT_EQ(ICompressionCodec::readMethod(encoded.data()), LZ4); /// below threshold -> deployment default
}

TEST(CompressionCodecAdaptive, DirectInvocationThrows)
{
    CompressionCodecAdaptive adaptive(*type("UInt32"), defaultCodec(), /*skip_threshold=*/0);
    /// Adaptive never appears on disk, so the public method byte accessor must reject direct use.
    EXPECT_ANY_THROW(adaptive.getMethodByte());
}

TEST(CompressionCodecAdaptive, ConcurrentCompressIsThreadSafe)
{
    CompressionCodecAdaptive adaptive(*type("UInt32"), defaultCodec(), /*skip_threshold=*/0);

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

TEST(GetCompressedBlockSize, PredictMatchesCompressForT64)
{
    std::vector<UInt32> values(50000);
    for (size_t i = 0; i < values.size(); ++i)
        values[i] = static_cast<UInt32>(i);
    auto bytes = bytesOf(values);
    const UInt32 size = static_cast<UInt32>(bytes.size());

    auto pool = AdaptiveCodec::poolForType(*type("UInt32"), defaultCodec());
    ASSERT_EQ(pool.size(), 2u);
    const auto & t64 = pool[1];
    ASSERT_EQ(t64->getMethodByte(), T64);

    PODArray<char> scratch;
    const UInt32 predicted = CompressedSizeEstimator::getCompressedBlockSize(*t64, bytes.data(), size, scratch);

    /// Re-derive size from a real compress: prediction must match exactly
    PODArray<char> encoded(t64->getCompressedReserveSize(size));
    const UInt32 actual = t64->compress(bytes.data(), size, encoded.data());
    EXPECT_EQ(predicted, actual);
}
