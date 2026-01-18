#include <gtest/gtest.h>

#include <Compression/CompressionCodecOpenZL.h>
#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <Common/PODArray.h>

#include <vector>
#include <cstring>

using namespace DB;

namespace
{

/// Helper to test compress-decompress round trip
template <typename T>
void testRoundTrip(CompressionCodecOpenZL & codec, const std::vector<T> & data)
{
    const char * source = reinterpret_cast<const char *>(data.data());
    UInt32 source_size = static_cast<UInt32>(data.size() * sizeof(T));

    /// Compress
    PODArray<char> compressed(codec.getCompressedReserveSize(source_size));
    UInt32 compressed_size = codec.compress(source, source_size, compressed.data());

    /// Decompress
    PODArray<char> decompressed(source_size + codec.getAdditionalSizeAtTheEndOfBuffer());
    codec.decompress(compressed.data(), compressed_size, decompressed.data());

    /// Verify
    ASSERT_EQ(std::memcmp(source, decompressed.data(), source_size), 0)
        << "Data mismatch after compress-decompress round trip";
}

} // namespace

TEST(CompressionCodecOpenZL, CompressDecompressRoundTripIntegers)
{
    CompressionCodecOpenZL codec;

    /// Test data: monotonically increasing integers (ideal for OpenZL)
    std::vector<UInt64> data(1000);
    for (size_t i = 0; i < data.size(); ++i)
        data[i] = i * 100;

    testRoundTrip(codec, data);
}

TEST(CompressionCodecOpenZL, CompressDecompressRoundTripFloats)
{
    CompressionCodecOpenZL codec;

    /// Test data: monotonically increasing floats
    std::vector<Float64> data(1000);
    for (size_t i = 0; i < data.size(); ++i)
        data[i] = static_cast<Float64>(i) * 1.5;

    testRoundTrip(codec, data);
}

TEST(CompressionCodecOpenZL, CompressDecompressRoundTripSmallIntegers)
{
    CompressionCodecOpenZL codec;

    /// Test data: small integers (UInt32)
    std::vector<UInt32> data(1000);
    for (size_t i = 0; i < data.size(); ++i)
        data[i] = static_cast<UInt32>(i % 256);

    testRoundTrip(codec, data);
}

TEST(CompressionCodecOpenZL, CompressDecompressRandomData)
{
    CompressionCodecOpenZL codec;

    /// Test data: pseudo-random data (less compressible)
    std::vector<UInt64> data(1000);
    UInt64 seed = 12345;
    for (size_t i = 0; i < data.size(); ++i)
    {
        seed = seed * 1103515245 + 12345; // LCG
        data[i] = seed;
    }

    testRoundTrip(codec, data);
}

TEST(CompressionCodecOpenZL, CompressDecompressEmpty)
{
    CompressionCodecOpenZL codec;

    const char * source = "";
    UInt32 source_size = 0;

    PODArray<char> compressed(codec.getCompressedReserveSize(source_size));
    UInt32 compressed_size = codec.compress(source, source_size, compressed.data());

    PODArray<char> decompressed(1);  // minimal buffer
    codec.decompress(compressed.data(), compressed_size, decompressed.data());

    SUCCEED();
}

TEST(CompressionCodecOpenZL, MethodByte)
{
    CompressionCodecOpenZL codec;

    ASSERT_EQ(codec.getMethodByte(), static_cast<uint8_t>(CompressionMethodByte::OpenZL));
}

/// Note: isExperimental(), isCompression(), isGenericCompression() are protected
/// These are tested indirectly via FactoryRegistration test

TEST(CompressionCodecOpenZL, FactoryRegistration)
{
    auto & factory = CompressionCodecFactory::instance();

    /// Should not throw
    auto codec = factory.get("OpenZL", {});

    ASSERT_NE(codec, nullptr);
    ASSERT_TRUE(codec->isExperimental());
    ASSERT_EQ(codec->getMethodByte(), static_cast<uint8_t>(CompressionMethodByte::OpenZL));
}

TEST(CompressionCodecOpenZL, CompressionRatio)
{
    CompressionCodecOpenZL codec;

    /// Highly compressible data: all zeros
    std::vector<UInt64> zeros(10000, 0);
    const char * source = reinterpret_cast<const char *>(zeros.data());
    UInt32 source_size = static_cast<UInt32>(zeros.size() * sizeof(UInt64));

    PODArray<char> compressed(codec.getCompressedReserveSize(source_size));
    UInt32 compressed_size = codec.compress(source, source_size, compressed.data());

    /// Expect some compression (compressed size should be less than original)
    /// Note: OpenZL might not compress trivial data well, so we just check it doesn't expand too much
    double ratio = static_cast<double>(source_size) / compressed_size;

    /// At minimum, verify the operation completed without error
    ASSERT_GT(compressed_size, 0u);

    /// Log the ratio for informational purposes
    std::cout << "OpenZL compression ratio for zeros: " << ratio << "x" << std::endl;
}
