#include <gtest/gtest.h>
#include <config.h>
#include <Storages/MergeTree/BitpackingBlockCodec.h>

#include <cstddef>
#include <random>
#include <span>
#include <vector>
#include <algorithm>


using Portable = DB::impl::BitpackingBlockCodecImpl<false>;
#if USE_SIMDCOMP
using SIMDComp = DB::impl::BitpackingBlockCodecImpl<true>;
#endif
namespace
{
std::vector<uint32_t> generateRandomData(size_t count, uint32_t max_bits, size_t seed = 42)
{
    std::mt19937 rng(static_cast<std::mt19937::result_type>(seed));
    std::vector<uint32_t> data(count);
    if (max_bits == 0)
    {
        std::fill(data.begin(), data.end(), 0);
        return data;
    }
    uint32_t max_val = (max_bits >= 32) ? 0xFFFFFFFFu : ((1u << max_bits) - 1);
    std::uniform_int_distribution<uint32_t> dist(0, max_val);
    for (auto & v : data)
        v = dist(rng);
    return data;
}

std::vector<uint32_t> generateZeroData(size_t count)
{
    return std::vector<uint32_t>(count, 0);
}

std::vector<uint32_t> generateMaxData(size_t count, uint32_t max_bits)
{
    uint32_t max_val = (max_bits >= 32) ? 0xFFFFFFFFu : ((1u << max_bits) - 1);
    return std::vector<uint32_t>(count, max_val);
}

void verifyRoundTrip(const std::vector<uint32_t> & original, std::optional<uint32_t> bits = std::nullopt)
{
    // Note: The bits parameter is kept for API compatibility but is no longer used
    // since the new BitpackingBlockCodec automatically determines optimal bit width
    (void)bits;

    std::span<uint32_t> data_span(const_cast<uint32_t*>(original.data()), original.size());

    size_t needed_bytes = Portable::calculateNeededBytes(data_span);

    std::vector<char> buffer(needed_bytes + 64, char(0xCC));
    std::span<char> out_span(buffer.data(), buffer.size());

    size_t used_encode = Portable::encode(data_span, out_span);

    ASSERT_EQ(static_cast<size_t>(used_encode), needed_bytes)
        << "encode used bytes must equal calculateNeededBytes()";
    ASSERT_EQ(out_span.size(), buffer.size() - needed_bytes)
        << "encode must advance output span by used bytes";

    std::vector<uint32_t> decoded(original.size(), 0xDEADBEEFu);
    std::span<uint32_t> decoded_span(decoded.data(), decoded.size());

    std::span<const std::byte> in_span(reinterpret_cast<const std::byte*>(buffer.data()), needed_bytes);

    size_t used_decode = Portable::decode(in_span, original.size(), decoded_span);

    ASSERT_EQ(used_decode, needed_bytes)
        << "decode used bytes must equal encode used bytes";
    ASSERT_EQ(in_span.size(), 0u)
        << "decode must consume exactly used bytes from input span";

    ASSERT_EQ(decoded, original) << "roundtrip mismatch";
}
}

// NOTE: Tests for private methods (maxbitsLength, bitpackingCompressedBytes, etc.) have been
// commented out since these are now internal implementation details. The public API is tested
// via the RoundTrip tests below.

/*
// Test maxbitsLength
TEST(PostingListCodecTest, MaxbitsLengthEmptyInput)
{
    std::vector<uint32_t> data;
    std::span<uint32_t> span(data);
    EXPECT_EQ(Portable::maxbitsLength(span), 0u);
}

TEST(PostingListCodecTest, MaxbitsLengthAllZeros)
{
    for (size_t count : {1, 4, 7, 16, 100, 128, 129, 256})
    {
        auto data = generateZeroData(count);
        std::span<uint32_t> span(data);
        EXPECT_EQ(Portable::maxbitsLength(span), 0u)
            << "Failed for count=" << count;
    }
}

TEST(PostingListCodecTest, MaxbitsLengthSingleBit)
{
    std::vector<uint32_t> data = {1, 0, 1, 0, 1};
    std::span<uint32_t> span(data);
    EXPECT_EQ(Portable::maxbitsLength(span), 1u);
}

TEST(PostingListCodecTest, MaxbitsLengthPowersOfTwoMinusOne)
{
    for (uint32_t bits = 1; bits <= 32; ++bits)
    {
        uint32_t value = (bits == 32) ? 0xFFFFFFFFu : ((1u << bits) - 1);
        std::vector<uint32_t> data = {value};
        std::span<uint32_t> span(data);
        EXPECT_EQ(Portable::maxbitsLength(span), bits)
            << "Failed for bits=" << bits << ", value=" << value;
    }
}

TEST(PostingListCodecTest, MaxbitsLengthExactPowerOfTwo)
{
    for (uint32_t bits = 1; bits <= 31; ++bits)
    {
        uint32_t value = 1u << bits;
        std::vector<uint32_t> data = {value};
        std::span<uint32_t> span(data);
        EXPECT_EQ(Portable::maxbitsLength(span), bits + 1)
            << "Failed for bits=" << bits << ", value=" << value;
    }
}

TEST(PostingListCodecTest, MaxbitsLengthMixedValues)
{
    std::vector<uint32_t> data = {0, 1, 2, 3, 255, 256};
    std::span<uint32_t> span(data);
    EXPECT_EQ(Portable::maxbitsLength(span), 9u);
}

TEST(PostingListCodecTest, MaxbitsLengthMaxUint32)
{
    std::vector<uint32_t> data = {0xFFFFFFFFu};
    std::span<uint32_t> span(data);
    EXPECT_EQ(Portable::maxbitsLength(span), 32u);
}

TEST(PostingListCodecTest, MaxbitsLengthTailHandling)
{
    for (size_t count = 1; count <= 10; ++count)
    {
        std::vector<uint32_t> data(count, 0);
        data.back() = 128;
        std::span<uint32_t> span(data);
        EXPECT_EQ(Portable::maxbitsLength(span), 8u)
            << "Failed for count=" << count;
    }
}

// Test bitpackingCompressedBytes
TEST(PostingListCodecTest, CompressedBytesZeroBits)
{
    for (size_t count : {0, 1, 100, 128, 1000})
    {
        EXPECT_EQ(Portable::bitpackingCompressedBytes(count, 0), 0u)
            << "Failed for count=" << count;
    }
}

TEST(PostingListCodecTest, CompressedBytesZeroCount)
{
    for (uint32_t bits = 0; bits <= 32; ++bits)
    {
        EXPECT_EQ(Portable::bitpackingCompressedBytes(0, bits), 0u)
            << "Failed for bits=" << bits;
    }
}

TEST(PostingListCodecTest, CompressedBytes32Bits)
{
    for (size_t count : {1, 4, 10, 100, 128})
    {
        size_t expected = count * sizeof(uint32_t);
        EXPECT_EQ(Portable::bitpackingCompressedBytes(count, 32), expected)
            << "Failed for count=" << count;
    }
}

TEST(PostingListCodecTest, CompressedBytesSingleGroup)
{
    for (uint32_t bits = 1; bits <= 31; ++bits)
    {
        size_t groups = 1;
        size_t words32 = (groups * bits + 31) / 32;
        size_t expected = words32 * 16;
        EXPECT_EQ(Portable::bitpackingCompressedBytes(4, bits), expected)
            << "Failed for bits=" << bits;
    }
}

TEST(PostingListCodecTest, CompressedBytesMultipleGroups)
{
    struct TestCase { size_t count; uint32_t bits; };
    std::vector<TestCase> cases = {
        {8, 16}, {12, 8}, {128, 1}, {128, 16}, {128, 31},
    };

    for (const auto & tc : cases)
    {
        size_t groups = (tc.count + 3) / 4;
        size_t words32 = (groups * tc.bits + 31) / 32;
        size_t expected = words32 * 16;
        EXPECT_EQ(Portable::bitpackingCompressedBytes(tc.count, tc.bits), expected)
            << "Failed for count=" << tc.count << ", bits=" << tc.bits;
    }
}

TEST(PostingListCodecTest, CompressedBytesNonMultipleOf4)
{
    for (size_t count = 1; count <= 20; ++count)
    {
        for (uint32_t bits : {1u, 8u, 16u, 31u})
        {
            size_t groups = (count + 3) / 4;
            size_t words32 = (groups * bits + 31) / 32;
            size_t expected = words32 * 16;
            EXPECT_EQ(Portable::bitpackingCompressedBytes(count, bits), expected)
                << "Failed for count=" << count << ", bits=" << bits;
        }
    }
}

TEST(PostingListCodecTest, CalculateNeededEmptyData)
{
    std::vector<uint32_t> data;
    std::span<uint32_t> span(data);
    auto [bytes, bits] = Portable::calculateNeededBytesAndMaxBits(span);
    EXPECT_EQ(bytes, 0u);
    EXPECT_EQ(bits, 0u);
}

TEST(PostingListCodecTest, CalculateNeededAllZeros)
{
    auto data = generateZeroData(100);
    std::span<uint32_t> span(data);
    auto [bytes, bits] = Portable::calculateNeededBytesAndMaxBits(span);
    EXPECT_EQ(bits, 0u);
    EXPECT_EQ(bytes, 0u);
}

TEST(PostingListCodecTest, CalculateNeededConsistency)
{
    for (size_t count : {1, 4, 7, 16, 100, 128, 129})
    {
        for (uint32_t target_bits : {1u, 8u, 16u, 24u, 32u})
        {
            auto data = generateRandomData(count, target_bits);
            std::span<uint32_t> span(data);

            auto [bytes, bits] = Portable::calculateNeededBytesAndMaxBits(span);
            uint32_t expected_bits = Portable::maxbitsLength(span);
            size_t expected_bytes = Portable::bitpackingCompressedBytes(count, expected_bits);

            EXPECT_EQ(bits, expected_bits);
            EXPECT_EQ(bytes, expected_bytes);
        }
    }
}

TEST(PostingListCodecTest, CalculateNeededSpecificValues)
{
    std::vector<uint32_t> data = {0, 1, 2, 3};
    std::span<uint32_t> span(data);
    auto [bytes, bits] = Portable::calculateNeededBytesAndMaxBits(span);
    EXPECT_EQ(bits, 2u);
    EXPECT_EQ(bytes, Portable::bitpackingCompressedBytes(4, 2));
}
*/

// RoundTrip tests - these test the public API
TEST(PostingListCodecTest, RoundTripZeroBits)
{
    for (size_t count : {1, 4, 7, 16, 100, 128, 129})
    {
        auto data = generateZeroData(count);
        verifyRoundTrip(data, 0);
    }
}

TEST(PostingListCodecTest, RoundTrip32Bits)
{
    for (size_t count : {1, 4, 7, 16, 100, 128, 129})
    {
        auto data = generateRandomData(count, 32);
        verifyRoundTrip(data, 32);
    }
}

TEST(PostingListCodecTest, RoundTripAllBitWidths)
{
    for (uint32_t bits = 1; bits <= 31; ++bits)
    {
        for (size_t count : {1, 4, 7, 16, 32, 100, 128, 129, 256})
        {
            auto data = generateRandomData(count, bits);
            verifyRoundTrip(data, bits);
        }
    }
}

TEST(PostingListCodecTest, RoundTripExactBlockSize)
{
    for (uint32_t bits : {1u, 8u, 16u, 24u, 31u, 32u})
    {
        for (size_t blocks : {1, 2, 3})
        {
            size_t count = blocks * 128;
            auto data = generateRandomData(count, bits);
            verifyRoundTrip(data, bits);
        }
    }
}

TEST(PostingListCodecTest, RoundTripBlockSizePlusTail)
{
    for (uint32_t bits : {1u, 8u, 16u, 31u, 32u})
    {
        for (size_t tail : {1, 2, 3, 4, 7, 63, 127})
        {
            size_t count = 128 + tail;
            auto data = generateRandomData(count, bits);
            verifyRoundTrip(data, bits);
        }
    }
}

TEST(PostingListCodecTest, RoundTripAllZerosWithNonZeroBits)
{
    for (uint32_t bits : {1u, 8u, 16u, 32u})
    {
        auto data = generateZeroData(100);
        verifyRoundTrip(data, bits);
    }
}

TEST(PostingListCodecTest, RoundTripMaxValues)
{
    for (uint32_t bits = 1; bits <= 32; ++bits)
    {
        auto data = generateMaxData(100, bits);
        verifyRoundTrip(data, bits);
    }
}

TEST(PostingListCodecTest, RoundTripSingleElement)
{
    for (uint32_t bits = 0; bits <= 32; ++bits)
    {
        uint32_t value = (bits == 0) ? 0 : ((bits >= 32) ? 0xFFFFFFFFu : ((1u << bits) - 1));
        std::vector<uint32_t> data = {value};
        verifyRoundTrip(data, bits);
    }
}

TEST(PostingListCodecTest, RoundTripTailOnly)
{
    for (size_t count = 1; count < 128; count += 7)
    {
        for (uint32_t bits : {1u, 8u, 16u, 31u, 32u})
        {
            auto data = generateRandomData(count, bits);
            verifyRoundTrip(data, bits);
        }
    }
}

TEST(PostingListCodecTest, LargeDataMultipleBlocks)
{
    for (uint32_t bits : {1u, 8u, 16u, 24u, 32u})
    {
        size_t count = 1024;
        auto data = generateRandomData(count, bits);
        verifyRoundTrip(data, bits);
    }
}

TEST(PostingListCodecTest, LargeDataWithTail)
{
    for (uint32_t bits : {1u, 8u, 16u, 31u})
    {
        size_t count = 1000;
        auto data = generateRandomData(count, bits);
        verifyRoundTrip(data, bits);
    }
}

TEST(PostingListCodecTest, StressRandomSizes)
{
    std::mt19937 rng(12345); // NOLINT(cert-msc32-c, cert-msc51-cpp)
    std::uniform_int_distribution<size_t> size_dist(1, 500);
    std::uniform_int_distribution<uint32_t> bits_dist(0, 32);

    for (int i = 0; i < 100; ++i)
    {
        size_t count = size_dist(rng);
        uint32_t bits = bits_dist(rng);
        auto data = generateRandomData(count, bits, rng());
        verifyRoundTrip(data, bits);
    }
}

TEST(PostingListCodecTest, StressAllCombinations)
{
    std::vector<size_t> sizes = {1, 2, 3, 4, 5, 7, 8, 15, 16, 31, 32, 63, 64, 127, 128, 129, 255, 256};

    for (uint32_t bits = 0; bits <= 32; ++bits)
    {
        for (size_t count : sizes)
        {
            auto data = generateRandomData(count, bits);
            verifyRoundTrip(data, bits);
        }
    }
}

TEST(PostingListCodecTest, BoundaryValuesCountTransitions)
{
    for (uint32_t bits : {8u, 16u})
    {
        for (size_t count : {3, 4, 5, 127, 128, 129, 255, 256, 257})
        {
            auto data = generateRandomData(count, bits);
            verifyRoundTrip(data, bits);
        }
    }
}

TEST(PostingListCodecTest, Empty)
{
    std::vector<uint32_t> values;
    verifyRoundTrip(values);
}

TEST(PostingListCodecTest, Bit32TightTailNoPadding)
{
    {
        std::vector<uint32_t> values = {0x80000000u};
        // The new API automatically determines bit width, so we just test the roundtrip
        verifyRoundTrip(values);
    }

    for (size_t n : {2u, 3u, 5u, 7u, 9u, 127u, 129u})
    {
        std::vector<uint32_t> values(n);
        uint32_t start = 0x80000000u - static_cast<uint32_t>(n - 1);
        for (size_t i = 0; i < n; ++i) values[i] = start + static_cast<uint32_t>(i);
        verifyRoundTrip(values);
    }
}

TEST(PostingListCodecTest, SmallBitsRandomMonotonicManySizes)
{
    std::mt19937 rng(12345); // NOLINT(cert-msc32-c, cert-msc51-cpp)
    std::uniform_int_distribution<uint32_t> delta_dist(0, 15);

    auto gen = [&](size_t n)
    {
        std::vector<uint32_t> v(n);
        uint32_t x = 0;
        for (size_t i = 0; i < n; ++i)
        {
            uint32_t d = delta_dist(rng);
            if (x + d >= (1u << 20)) d = 0;
            x += d;
            v[i] = x;
        }
        return v;
    };

    for (size_t n: {1u, 2u, 3u, 4u, 5u, 31u, 32u, 33u, 63u, 64u, 65u, 127u, 128u, 129u, 511u})
    {
        verifyRoundTrip(gen(n));
    }
}

TEST(PostingListCodecTest, MixedRandomMonotonicLarger)
{
    std::mt19937 rng(20240601); // NOLINT(cert-msc32-c, cert-msc51-cpp)
    std::uniform_int_distribution<uint32_t> delta_dist(0, 100000);

    for (int t = 0; t < 20; ++t)
    {
        size_t n = 1000 + size_t(t) * 37;
        std::vector<uint32_t> values(n);
        uint64_t x = 0;
        for (size_t i = 0; i < n; ++i)
        {
            x += delta_dist(rng);
            x = std::min<uint64_t>(x, 0xFFFFFFFFull);
            values[i] = static_cast<uint32_t>(x);
        }
        verifyRoundTrip(values);
    }
}

[[maybe_unused]] static size_t expectedCompressedBytes(size_t length, uint32_t bit)
{
    if (length == 0) return 0;
    if (bit == 0) return 1; // 1 byte header only
    if (bit == 32) return 1 + length * sizeof(uint32_t); // 1 byte header + raw data
    const size_t groups = (length + 3) / 4;
    const size_t words32 = (groups * size_t(bit) + 31) / 32;
    return 1 + words32 * 16; // 1 byte header + payload
}

[[maybe_unused]] static uint32_t maskForBit(uint32_t bit)
{
    if (bit == 0) return 0u;
    if (bit == 32) return 0xFFFFFFFFu;
    if (bit == 31) return 0x7FFFFFFFu;
    return (uint32_t(1) << bit) - 1u;
}

/// Calculate the actual max bits needed to represent the data.
/// This mirrors the calculation done inside the encoder.
[[maybe_unused]] static uint32_t calculateMaxBits(const std::vector<uint32_t> & data)
{
    if (data.empty())
        return 0;
    uint32_t xored = 0;
    for (auto v : data)
        xored |= v;
    if (xored == 0)
        return 0;
    return 32u - static_cast<uint32_t>(__builtin_clz(xored));
}

[[maybe_unused]] static std::vector<uint32_t> makeAllZeros(size_t n)
{
    return std::vector<uint32_t>(n, 0u);
}

[[maybe_unused]] static std::vector<uint32_t> makeAllMax(size_t n, uint32_t bit)
{
    return std::vector<uint32_t>(n, maskForBit(bit));
}

[[maybe_unused]] static std::vector<uint32_t> makeIncreasing(size_t n, uint32_t bit)
{
    std::vector<uint32_t> v(n);
    uint32_t m = maskForBit(bit);
    uint32_t cur = 0;
    for (size_t i = 0; i < n; ++i)
    {
        if (bit == 32)
        {
            cur = cur + 1;
        } else
        {
            cur = (cur + 1) & m;
        }
        v[i] = cur;
    }
    return v;
}

[[maybe_unused]] static std::vector<uint32_t> makeRandom(size_t n, uint32_t bit, uint32_t seed)
{
    std::mt19937 rng(seed);
    std::vector<uint32_t> v(n);
    uint32_t m = maskForBit(bit);

    if (bit == 32)
    {
        std::uniform_int_distribution<uint32_t> dist(0, 0xFFFFFFFFu);
        for (auto &x: v) x = dist(rng);
    } else
    {
        std::uniform_int_distribution<uint32_t> dist(0, m);
        for (auto &x: v) x = dist(rng);
    }
    sort(v.begin(), v.end());
    return v;
}

// Portable encode helper: encodes into `out` sized exactly to the expected payload length.
// Note: The `bit` parameter is ignored; the actual max bits is computed from the data.
[[maybe_unused]] static size_t encodePortable(const std::vector<uint32_t> & in, uint32_t /*bit*/, std::vector<std::byte> & out)
{
    const uint32_t actual_bits = calculateMaxBits(in);
    const size_t expected_bytes = expectedCompressedBytes(in.size(), actual_bits);
    out.assign(expected_bytes, std::byte{0});

    std::span<uint32_t> in_span(const_cast<uint32_t*>(in.data()), in.size());
    std::span<char> out_span(reinterpret_cast<char*>(out.data()), out.size());

    const size_t used = Portable::encode(in_span, out_span);
    EXPECT_EQ(size_t(used), expected_bytes) << "Portable encode used-bytes must match the format formula";
    return used;
}

// Portable decode helper: decodes `n` integers from `in` into `out`.
// Note: The `bit` parameter is ignored; the decoder reads the bit width from the header.
[[maybe_unused]] static size_t decodePortable(const std::vector<std::byte> & in, size_t n, uint32_t /*bit*/, std::vector<uint32_t> & out)
{
    out.assign(n, 0u);

    std::span<const std::byte> in_span(in.data(), in.size());
    std::span<uint32_t> out_span(out.data(), out.size());

    const size_t used = Portable::decode(in_span, n, out_span);
    // Note: we cannot verify expected_bytes here because we don't know the actual bits
    // used during encoding. The decode will consume exactly what was written.
    EXPECT_EQ(in_span.size(), 0u) << "Portable decode must consume the entire payload span";
    return used;
}

#if USE_SIMDCOMP
// SIMDComp encode helper: encodes into `out` sized exactly to the expected payload length.
// Note: The `bit` parameter is ignored; the actual max bits is computed from the data.
static size_t encodeSIMDComp(const std::vector<uint32_t> & in, uint32_t /*bit*/, std::vector<std::byte> & out)
{
    const uint32_t actual_bits = calculateMaxBits(in);
    const size_t expected_bytes = expectedCompressedBytes(in.size(), actual_bits);
    out.assign(expected_bytes, std::byte{0});

    std::span<uint32_t> in_span(const_cast<uint32_t*>(in.data()), in.size());
    std::span<char> out_span(reinterpret_cast<char*>(out.data()), out.size());

    const size_t used = SIMDComp::encode(in_span, out_span);
    EXPECT_EQ(used, expected_bytes) << "SIMDComp encode used-bytes must match the format formula";
    return used;
}

// SIMDComp decode helper: decodes `n` integers from `in` into `out`.
// Note: The `bit` parameter is ignored; the decoder reads the bit width from the header.
static size_t decodeSIMDComp(const std::vector<std::byte> & in, size_t n, uint32_t /*bit*/, std::vector<uint32_t> & out)
{
    out.assign(n, 0u);

    std::span<const std::byte> in_span(in.data(), in.size());
    std::span<uint32_t> out_span(out.data(), out.size());

    const size_t used = SIMDComp::decode(in_span, n, out_span);
    // Note: we cannot verify expected_bytes here because we don't know the actual bits
    // used during encoding. The decode will consume exactly what was written.
    EXPECT_EQ(in_span.size(), 0u) << "SIMDComp decode must consume the entire payload span";
    return used;
}
#endif



// -----------------------------------------------------------------------------
//  Byte-for-byte equality of the encoded payload
// -----------------------------------------------------------------------------
TEST(PostingListCodecTest, EncodeBytesMatchSSEvsPortable)
{
#if USE_SIMDCOMP
    const std::vector<size_t> lengths = {0,1,2,3,4,5,31,32,33,127,128,129,255,256,257};
    const std::vector<uint32_t> bits  = {0,1,2,3,7,8,9,15,16,17,30,31,32};

    for (size_t n : lengths)
    {
        for (uint32_t bit: bits)
        {
            const auto input = makeRandom(n, bit, uint32_t(1234 + n * 97 + bit));

            // Use actual max bits from the data, not the test parameter
            const uint32_t actual_bits = calculateMaxBits(input);
            const size_t expected_bytes = expectedCompressedBytes(n, actual_bits);

            std::vector<std::byte> enc_port(expected_bytes, std::byte{0});
            std::vector<std::byte> enc_sse(expected_bytes, std::byte{0});

            const size_t used_port = encodePortable(input, bit, enc_port);
            const size_t used_sse = encodeSIMDComp(input, bit, enc_sse);

            ASSERT_EQ(used_sse, expected_bytes) << "SSE encode used-bytes mismatch the format formula";
            ASSERT_EQ(used_port, expected_bytes);

            ASSERT_EQ(enc_port.size(), enc_sse.size());
            if (expected_bytes > 0)
                ASSERT_EQ(0, std::memcmp(enc_port.data(), enc_sse.data(), expected_bytes))
                    << "Encoded byte stream differs (n=" << n << ", bit=" << bit << ")";
        }
    }
#else
    GTEST_SKIP() << "SSE not available on this platform.";
#endif
}

// -----------------------------------------------------------------------------
//  Cross-decoding (SSE->Portable and Portable->SSE)
// -----------------------------------------------------------------------------
TEST(PostingListCodecTest, CrossDecodeSSEtoPortableandBack)
{
#if USE_SIMDCOMP
    const std::vector<size_t> lengths = {1,2,3,4,5,127,128,129,257};
    const std::vector<uint32_t> bits  = {0,1,2,5,9,16,17,31,32};

    for (size_t n: lengths)
    {
        for (uint32_t bit: bits)
        {
            const auto input = makeIncreasing(n, bit);

            // Use actual max bits from the data, not the test parameter
            const uint32_t actual_bits = calculateMaxBits(input);
            const size_t expected_bytes = expectedCompressedBytes(n, actual_bits);

            // SSE encode -> Portable decode
            std::vector<std::byte> enc_sse(expected_bytes, std::byte{0});
            const size_t used_sse = encodeSIMDComp(input, bit, enc_sse);
            ASSERT_EQ(size_t(used_sse), expected_bytes);

            std::vector<uint32_t> dec_port;
            decodePortable(enc_sse, n, bit, dec_port);
            ASSERT_EQ(input, dec_port) << "SSE->Portable decode mismatch (n=" << n << ", bit=" << bit << ")";

            // Portable encode -> SSE decode
            std::vector<std::byte> enc_port;
            encodePortable(input, bit, enc_port);

            std::vector<uint32_t> dec_sse(n, 0u);
            const size_t used_dec_sse = decodeSIMDComp(enc_port, n, bit, dec_sse);
            ASSERT_EQ(used_dec_sse, expected_bytes);
            ASSERT_EQ(input, dec_sse) << "Portable->SSE decode mismatch (n=" << n << ", bit=" << bit << ")";
        }
    }
#else
    GTEST_SKIP() << "SSE not available on this platform.";
#endif
}

// -----------------------------------------------------------------------------
//  Hand-picked edge cases and patterns that often expose bugs
// -----------------------------------------------------------------------------
TEST(PostingListCodecTest, SpecialPatterns)
{
#if USE_SIMDCOMP
    struct Case
    {
        size_t n;
        uint32_t bit;
    };
    const std::vector<Case> cases = {
        {0, 0}, {1, 0}, {128, 0},
        {1, 32}, {3, 32}, {127, 32}, {128, 32}, {129, 32},
        {127, 31}, {128, 31}, {129, 31},
        {127, 17}, {128, 17}, {129, 17},
        {5, 1}, {5, 2}, {5, 3}, {5, 30}
    };

    for (const auto & c: cases)
    {
        for (int pat = 0; pat < 3; ++pat)
        {
            std::vector<uint32_t> input;
            if (pat == 0) input = makeAllZeros(c.n);
            if (pat == 1) input = makeAllMax(c.n, c.bit);
            if (pat == 2) input = makeRandom(c.n, c.bit, 2025u + uint32_t(c.n) + c.bit);

            // Use actual max bits from the data, not the test parameter
            const uint32_t actual_bits = calculateMaxBits(input);
            const size_t expected_bytes = expectedCompressedBytes(c.n, actual_bits);

            std::vector<std::byte> enc_port, enc_sse(expected_bytes, std::byte{0});

            encodePortable(input, c.bit, enc_port);
            const size_t used_sse = encodeSIMDComp(input, c.bit, enc_sse);
            ASSERT_EQ(size_t(used_sse), expected_bytes);

            ASSERT_EQ(enc_port.size(), expected_bytes);
            ASSERT_EQ(enc_sse.size(), expected_bytes);

            if (expected_bytes > 0)
                ASSERT_EQ(0, std::memcmp(enc_port.data(), enc_sse.data(), expected_bytes))
                    << "Pattern differs (pattern=" << pat << ", n=" << c.n << ", bit=" << c.bit << ")";
        }
    }
#else
    GTEST_SKIP() << "SSE not available on this platform.";
#endif
}

TEST(PostingListCodecTest, PortableEncodeDecodedBySSEAndSSEEncodeDecodedByPortable)
{
#if USE_SIMDCOMP
    // Sizes chosen to exercise:
    // - small tails (1,2,3 mod 4)
    // - near block boundaries (127/128/129)
    // - multi-block scenarios (256+)
    const std::vector<size_t> lengths = {1, 2, 3, 5, 33, 127, 128, 129, 257};

    // Bits chosen to exercise:
    // - bit==0 (no payload)
    // - narrow widths
    // - boundary widths
    // - bit==32 (tight uint32_t stream)
    const std::vector<uint32_t> bits = {0, 1, 2, 7, 8, 9, 16, 31, 32};

    for (size_t n: lengths)
    {
        for (uint32_t bit: bits)
        {
            // Use deterministic but non-trivial data.
            // Increasing is close to posting list use-cases and still stresses boundaries.
            auto input = makeIncreasing(n, bit);

            // Use actual max bits from the data, not the test parameter
            const uint32_t actual_bits = calculateMaxBits(input);
            const size_t expected_bytes = expectedCompressedBytes(n, actual_bits);

            // -----------------------------------------------------------------
            // Direction 1: Portable encode -> SSE decode
            // -----------------------------------------------------------------
            std::vector<std::byte> enc_port;
            encodePortable(input, bit, enc_port);
            ASSERT_EQ(enc_port.size(), expected_bytes);

            std::vector<uint32_t> dec_sse(n, 0u);
            std::span<const std::byte> enc_port_span(reinterpret_cast<const std::byte*>(enc_port.data()), enc_port.size());
            std::span<uint32_t> dec_sse_span(dec_sse.data(), dec_sse.size());
            const size_t used_dec_sse = SIMDComp::decode(enc_port_span, n, dec_sse_span);
            ASSERT_EQ(used_dec_sse, expected_bytes) << "SSE decode consumed-bytes mismatch (portable payload)";
            ASSERT_EQ(input, dec_sse) << "Portable-encoded payload did not decode correctly in SSE path"
                    << " (n=" << n << ", bit=" << bit << ")";

            // -----------------------------------------------------------------
            // Direction 2: SSE encode -> Portable decode
            // -----------------------------------------------------------------
            std::vector<std::byte> enc_sse(expected_bytes, std::byte{0});
            std::span<uint32_t> input_span(input.data(), input.size());
            std::span<char> enc_sse_span(reinterpret_cast<char*>(enc_sse.data()), enc_sse.size());
            const size_t used_enc_sse = SIMDComp::encode(input_span, enc_sse_span);
            ASSERT_EQ(size_t(used_enc_sse), expected_bytes) << "SSE encode used-bytes mismatch";

            std::vector<uint32_t> dec_port;
            decodePortable(enc_sse, n, bit, dec_port);
            ASSERT_EQ(input, dec_port) << "SSE-encoded payload did not decode correctly in portable path"
                    << " (n=" << n << ", bit=" << bit << ")";
        }
    }
#else
    GTEST_SKIP() << "SSE not available on this platform.";
#endif
}

// =============================================================================
// FastPFor Codec Tests
// =============================================================================
// These tests cover the FastPFor library-based codecs:
// - SIMDFastPForBlockCodec (fastpfor)
// - SIMDBinaryPackingBlockCodec (binarypacking)
// - SIMDOptPForBlockCodec (optpfor)

#if USE_FASTPFOR
#include <Storages/MergeTree/FastPForBlockCodec.h>

namespace
{

/// Helper to verify round-trip encoding/decoding for FastPFor codecs.
/// Template parameter Codec should be one of the FastPFor codec types.
template <typename Codec>
void verifyFastPForRoundTrip(const std::vector<uint32_t> & original)
{
    if (original.empty())
        return;

    std::vector<uint32_t> input = original;  // Make a mutable copy
    std::span<uint32_t> in_span(input.data(), input.size());

    // Calculate needed bytes and allocate buffer
    size_t needed_bytes = Codec::calculateNeededBytes(in_span);
    std::vector<char> buffer(needed_bytes + 256, char(0xCC));  // Extra padding for safety
    std::span<char> out_span(buffer.data(), buffer.size());

    // Encode
    size_t used_encode = Codec::encode(in_span, out_span);
    ASSERT_GT(used_encode, 0u) << Codec::name() << ": encode should produce non-zero bytes for non-empty input";
    ASSERT_LE(used_encode, needed_bytes + 256) << Codec::name() << ": encode exceeded buffer size";

    // Decode
    std::vector<uint32_t> decoded(original.size(), 0xDEADBEEFu);
    std::span<uint32_t> decoded_span(decoded.data(), decoded.size());
    std::span<const std::byte> decode_in(reinterpret_cast<const std::byte*>(buffer.data()), used_encode);

    size_t used_decode = Codec::decode(decode_in, original.size(), decoded_span);
    ASSERT_EQ(used_decode, used_encode)
        << Codec::name() << ": decode consumed bytes must equal encode written bytes";

    // Verify data integrity
    ASSERT_EQ(decoded, original)
        << Codec::name() << ": roundtrip mismatch for " << original.size() << " elements";
}

/// Generate monotonically increasing data (simulates posting list row IDs)
std::vector<uint32_t> generateMonotonicData(size_t count, uint32_t gap, uint32_t seed = 42)
{
    std::mt19937 rng(static_cast<std::mt19937::result_type>(seed));
    std::uniform_int_distribution<uint32_t> jitter(0, gap / 2);

    std::vector<uint32_t> data(count);
    uint32_t value = 0;
    for (size_t i = 0; i < count; ++i)
    {
        value += gap + jitter(rng);
        data[i] = value;
    }
    return data;
}

/// Generate delta-encoded data (what posting list codecs actually compress)
std::vector<uint32_t> generateDeltaData(size_t count, uint32_t max_delta, uint32_t seed = 42)
{
    std::mt19937 rng(static_cast<std::mt19937::result_type>(seed));
    std::uniform_int_distribution<uint32_t> dist(1, max_delta);

    std::vector<uint32_t> data(count);
    for (size_t i = 0; i < count; ++i)
        data[i] = dist(rng);
    return data;
}

/// Generate data with outliers (tests patching in PFor codecs)
std::vector<uint32_t> generateDataWithOutliers(size_t count, uint32_t base_max, uint32_t outlier_value, double outlier_ratio, uint32_t seed = 42)
{
    std::mt19937 rng(static_cast<std::mt19937::result_type>(seed));
    std::uniform_int_distribution<uint32_t> base_dist(1, base_max);
    std::uniform_real_distribution<double> outlier_dist(0.0, 1.0);

    std::vector<uint32_t> data(count);
    for (size_t i = 0; i < count; ++i)
    {
        if (outlier_dist(rng) < outlier_ratio)
            data[i] = outlier_value;
        else
            data[i] = base_dist(rng);
    }
    return data;
}

}  // anonymous namespace

// -----------------------------------------------------------------------------
// SIMDFastPForBlockCodec Tests
// -----------------------------------------------------------------------------

TEST(FastPForCodecTest, FastPForRoundTripEmpty)
{
    std::vector<uint32_t> empty;
    // Empty input should not crash
    std::span<uint32_t> in_span(empty.data(), empty.size());
    EXPECT_EQ(DB::SIMDFastPForBlockCodec::calculateNeededBytes(in_span), 0u);
}

TEST(FastPForCodecTest, FastPForRoundTripSmallSizes)
{
    for (size_t count : {1, 2, 3, 4, 5, 7, 8, 15, 16, 31, 32, 63, 64, 100, 127, 128, 129})
    {
        auto data = generateDeltaData(count, 1000);
        verifyFastPForRoundTrip<DB::SIMDFastPForBlockCodec>(data);
    }
}

TEST(FastPForCodecTest, FastPForRoundTripBlockBoundaries)
{
    // Test exact block boundaries and near-boundaries
    // FastPFor BLOCK_SIZE is 128
    for (size_t count : {127, 128, 129, 255, 256, 257, 383, 384, 385, 511, 512, 513})
    {
        auto data = generateDeltaData(count, 500);
        verifyFastPForRoundTrip<DB::SIMDFastPForBlockCodec>(data);
    }
}

TEST(FastPForCodecTest, FastPForRoundTripLargeSizes)
{
    for (size_t count : {1000, 2000, 5000, 10000})
    {
        auto data = generateDeltaData(count, 1000);
        verifyFastPForRoundTrip<DB::SIMDFastPForBlockCodec>(data);
    }
}

TEST(FastPForCodecTest, FastPForRoundTripAllZeros)
{
    for (size_t count : {1, 4, 128, 129, 256, 1000})
    {
        std::vector<uint32_t> zeros(count, 0);
        verifyFastPForRoundTrip<DB::SIMDFastPForBlockCodec>(zeros);
    }
}

TEST(FastPForCodecTest, FastPForRoundTripAllOnes)
{
    for (size_t count : {1, 4, 128, 129, 256, 1000})
    {
        std::vector<uint32_t> ones(count, 1);
        verifyFastPForRoundTrip<DB::SIMDFastPForBlockCodec>(ones);
    }
}

TEST(FastPForCodecTest, FastPForRoundTripMaxValues)
{
    for (size_t count : {1, 4, 128, 129, 256})
    {
        std::vector<uint32_t> max_vals(count, 0xFFFFFFFFu);
        verifyFastPForRoundTrip<DB::SIMDFastPForBlockCodec>(max_vals);
    }
}

TEST(FastPForCodecTest, FastPForRoundTripWithOutliers)
{
    // FastPFor should handle outliers well via patching
    for (size_t count : {128, 256, 512, 1000})
    {
        // 5% outliers with large values
        auto data = generateDataWithOutliers(count, 100, 1000000, 0.05);
        verifyFastPForRoundTrip<DB::SIMDFastPForBlockCodec>(data);
    }
}

TEST(FastPForCodecTest, FastPForRoundTripMonotonic)
{
    // Simulates actual posting list use case
    for (size_t count : {100, 500, 1000, 5000})
    {
        auto data = generateMonotonicData(count, 10);
        verifyFastPForRoundTrip<DB::SIMDFastPForBlockCodec>(data);
    }
}

// -----------------------------------------------------------------------------
// SIMDBinaryPackingBlockCodec Tests
// -----------------------------------------------------------------------------

TEST(FastPForCodecTest, BinaryPackingRoundTripSmallSizes)
{
    for (size_t count : {1, 2, 3, 4, 5, 7, 8, 15, 16, 31, 32, 63, 64, 100, 127, 128, 129})
    {
        auto data = generateDeltaData(count, 1000);
        verifyFastPForRoundTrip<DB::SIMDBinaryPackingBlockCodec>(data);
    }
}

TEST(FastPForCodecTest, BinaryPackingRoundTripBlockBoundaries)
{
    for (size_t count : {127, 128, 129, 255, 256, 257, 383, 384, 385})
    {
        auto data = generateDeltaData(count, 500);
        verifyFastPForRoundTrip<DB::SIMDBinaryPackingBlockCodec>(data);
    }
}

TEST(FastPForCodecTest, BinaryPackingRoundTripLargeSizes)
{
    for (size_t count : {1000, 2000, 5000})
    {
        auto data = generateDeltaData(count, 1000);
        verifyFastPForRoundTrip<DB::SIMDBinaryPackingBlockCodec>(data);
    }
}

TEST(FastPForCodecTest, BinaryPackingRoundTripAllZeros)
{
    for (size_t count : {1, 4, 128, 129, 256})
    {
        std::vector<uint32_t> zeros(count, 0);
        verifyFastPForRoundTrip<DB::SIMDBinaryPackingBlockCodec>(zeros);
    }
}

TEST(FastPForCodecTest, BinaryPackingRoundTripMaxValues)
{
    for (size_t count : {1, 4, 128, 129, 256})
    {
        std::vector<uint32_t> max_vals(count, 0xFFFFFFFFu);
        verifyFastPForRoundTrip<DB::SIMDBinaryPackingBlockCodec>(max_vals);
    }
}

// -----------------------------------------------------------------------------
// SIMDOptPForBlockCodec Tests
// -----------------------------------------------------------------------------

TEST(FastPForCodecTest, OptPForRoundTripSmallSizes)
{
    for (size_t count : {1, 2, 3, 4, 5, 7, 8, 15, 16, 31, 32, 63, 64, 100, 127, 128, 129})
    {
        auto data = generateDeltaData(count, 1000);
        verifyFastPForRoundTrip<DB::SIMDOptPForBlockCodec>(data);
    }
}

TEST(FastPForCodecTest, OptPForRoundTripBlockBoundaries)
{
    for (size_t count : {127, 128, 129, 255, 256, 257, 383, 384, 385})
    {
        auto data = generateDeltaData(count, 500);
        verifyFastPForRoundTrip<DB::SIMDOptPForBlockCodec>(data);
    }
}

TEST(FastPForCodecTest, OptPForRoundTripLargeSizes)
{
    for (size_t count : {1000, 2000, 5000})
    {
        auto data = generateDeltaData(count, 1000);
        verifyFastPForRoundTrip<DB::SIMDOptPForBlockCodec>(data);
    }
}

TEST(FastPForCodecTest, OptPForRoundTripWithOutliers)
{
    // OptPFor should handle outliers very well (optimized patching)
    for (size_t count : {128, 256, 512, 1000})
    {
        // 10% outliers - OptPFor should still compress well
        auto data = generateDataWithOutliers(count, 100, 1000000, 0.10);
        verifyFastPForRoundTrip<DB::SIMDOptPForBlockCodec>(data);
    }
}

TEST(FastPForCodecTest, OptPForRoundTripAllZeros)
{
    for (size_t count : {1, 4, 128, 129, 256})
    {
        std::vector<uint32_t> zeros(count, 0);
        verifyFastPForRoundTrip<DB::SIMDOptPForBlockCodec>(zeros);
    }
}

TEST(FastPForCodecTest, OptPForRoundTripMaxValues)
{
    for (size_t count : {1, 4, 128, 129, 256})
    {
        std::vector<uint32_t> max_vals(count, 0xFFFFFFFFu);
        verifyFastPForRoundTrip<DB::SIMDOptPForBlockCodec>(max_vals);
    }
}

// -----------------------------------------------------------------------------
// Cross-Codec Consistency Tests
// -----------------------------------------------------------------------------

TEST(FastPForCodecTest, AllCodecsProduceSameDecodedOutput)
{
    // Verify that all codecs decode to the same original data
    for (size_t count : {100, 128, 129, 256, 500, 1000})
    {
        auto original = generateDeltaData(count, 1000, 12345);

        // Encode and decode with each codec
        auto testCodec = [&original](auto & codec_type) {
            using Codec = std::decay_t<decltype(codec_type)>;

            std::vector<uint32_t> input = original;
            std::span<uint32_t> in_span(input.data(), input.size());

            size_t needed = Codec::calculateNeededBytes(in_span);
            std::vector<char> buffer(needed + 256);
            std::span<char> out_span(buffer.data(), buffer.size());

            Codec::encode(in_span, out_span);

            std::vector<uint32_t> decoded(original.size());
            std::span<uint32_t> dec_span(decoded.data(), decoded.size());
            std::span<const std::byte> dec_in(reinterpret_cast<const std::byte*>(buffer.data()), buffer.size());

            Codec::decode(dec_in, original.size(), dec_span);
            return decoded;
        };

        DB::SIMDFastPForBlockCodec fastpfor;
        DB::SIMDBinaryPackingBlockCodec binarypacking;
        DB::SIMDOptPForBlockCodec optpfor;

        auto dec_fastpfor = testCodec(fastpfor);
        auto dec_binarypacking = testCodec(binarypacking);
        auto dec_optpfor = testCodec(optpfor);

        ASSERT_EQ(dec_fastpfor, original) << "FastPFor decode mismatch at count=" << count;
        ASSERT_EQ(dec_binarypacking, original) << "BinaryPacking decode mismatch at count=" << count;
        ASSERT_EQ(dec_optpfor, original) << "OptPFor decode mismatch at count=" << count;
    }
}

// -----------------------------------------------------------------------------
// Stress Tests
// -----------------------------------------------------------------------------

TEST(FastPForCodecTest, StressRandomSizesAllCodecs)
{
    std::mt19937 rng(98765);
    std::uniform_int_distribution<size_t> size_dist(1, 2000);
    std::uniform_int_distribution<uint32_t> delta_dist(1, 10000);

    for (int i = 0; i < 50; ++i)
    {
        size_t count = size_dist(rng);
        uint32_t max_delta = delta_dist(rng);
        auto data = generateDeltaData(count, max_delta, static_cast<uint32_t>(rng()));

        verifyFastPForRoundTrip<DB::SIMDFastPForBlockCodec>(data);
        verifyFastPForRoundTrip<DB::SIMDBinaryPackingBlockCodec>(data);
        verifyFastPForRoundTrip<DB::SIMDOptPForBlockCodec>(data);
    }
}

TEST(FastPForCodecTest, StressMonotonicDataAllCodecs)
{
    // Simulates real posting list behavior
    std::mt19937 rng(54321);
    std::uniform_int_distribution<size_t> size_dist(100, 5000);
    std::uniform_int_distribution<uint32_t> gap_dist(1, 100);

    for (int i = 0; i < 20; ++i)
    {
        size_t count = size_dist(rng);
        uint32_t gap = gap_dist(rng);
        auto data = generateMonotonicData(count, gap, static_cast<uint32_t>(rng()));

        verifyFastPForRoundTrip<DB::SIMDFastPForBlockCodec>(data);
        verifyFastPForRoundTrip<DB::SIMDBinaryPackingBlockCodec>(data);
        verifyFastPForRoundTrip<DB::SIMDOptPForBlockCodec>(data);
    }
}

#endif // USE_FASTPFOR
