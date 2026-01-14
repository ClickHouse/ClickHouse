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

static void assertRoundTripIsOkay(std::vector<uint32_t> data)
{
    std::span<uint32_t> data_span(data.data(), data.size());

    auto [needed_bytes, max_bits] = Portable::calculateNeededBytesAndMaxBits(data_span);

    std::vector<char> buffer(needed_bytes + 64, char(0xCC));
    std::span<char> out_span(buffer.data(), buffer.size());

    uint32_t used_encode = Portable::encode(data_span, static_cast<uint32_t>(max_bits), out_span);

    ASSERT_EQ(static_cast<size_t>(used_encode), needed_bytes)
        << "encode used bytes must equal calculateNeededBytesAndMaxBits().first";
    ASSERT_EQ(out_span.size(), buffer.size() - needed_bytes)
        << "encode must advance output span by used bytes";

    std::vector<uint32_t> decoded(data.size(), 0xDEADBEEFu);
    std::span<uint32_t> decoded_span(decoded.data(), decoded.size());

    std::span<const std::byte> in_span(reinterpret_cast<const std::byte*>(buffer.data()), needed_bytes);

    size_t used_decode = Portable::decode(in_span, data.size(), static_cast<uint32_t>(max_bits), decoded_span);

    ASSERT_EQ(used_decode, needed_bytes)
        << "decode used bytes must equal encode used bytes";
    ASSERT_EQ(in_span.size(), 0u)
        << "decode must consume exactly used bytes from input span";

    ASSERT_EQ(decoded, data) << "roundtrip mismatch";
}

TEST(PostingListCodecTest, Empty)
{
    std::vector<uint32_t> values;
    assertRoundTripIsOkay(values);
}

TEST(PostingListCodecTest, Bit32TightTailNoPadding)
{
    {
        std::vector<uint32_t> values = {0x80000000u};
        std::span<uint32_t> span(values.data(), values.size());
        auto [needed_bytes, max_bits] = Portable::calculateNeededBytesAndMaxBits(span);

        ASSERT_EQ(max_bits, 32u);
        ASSERT_EQ(needed_bytes, 4u) << "bit==32 tail must be tight (length*4), not padded to 16B";

        assertRoundTripIsOkay(values);
    }

    for (size_t n : {2u, 3u, 5u, 7u, 9u, 127u, 129u})
    {
        std::vector<uint32_t> values(n);
        uint32_t start = 0x80000000u - static_cast<uint32_t>(n - 1);
        for (size_t i = 0; i < n; ++i) values[i] = start + static_cast<uint32_t>(i);
        assertRoundTripIsOkay(values);
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
        assertRoundTripIsOkay(gen(n));
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
        assertRoundTripIsOkay(std::move(values));
    }
}

[[maybe_unused]] static size_t expectedCompressedBytes(size_t length, uint32_t bit)
{
    if (bit == 0) return 0;
    if (bit == 32) return length * sizeof(uint32_t);
    const size_t groups = (length + 3) / 4;
    const size_t words32 = (groups * size_t(bit) + 31) / 32;
    return words32 * 16; // 16 bytes
}

[[maybe_unused]] static uint32_t maskForBit(uint32_t bit)
{
    if (bit == 0) return 0u;
    if (bit == 32) return 0xFFFFFFFFu;
    if (bit == 31) return 0x7FFFFFFFu;
    return (uint32_t(1) << bit) - 1u;
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
[[maybe_unused]] static uint32_t encodePortable(const std::vector<uint32_t> & in, uint32_t bit, std::vector<std::byte> & out)
{
    const size_t expected_bytes = expectedCompressedBytes(in.size(), bit);
    out.assign(expected_bytes, std::byte{0});

    std::span<uint32_t> in_span(const_cast<uint32_t*>(in.data()), in.size());
    std::span<char> out_span(reinterpret_cast<char*>(out.data()), out.size());

    const uint32_t used = Portable::encode(in_span, bit, out_span);
    EXPECT_EQ(size_t(used), expected_bytes) << "Portable encode used-bytes must match the format formula";
    return used;
}

// Portable decode helper: decodes `n` integers from `in` (expected payload length) into `out`.
[[maybe_unused]] static size_t decodePortable(const std::vector<std::byte> & in, size_t n, uint32_t bit, std::vector<uint32_t> & out)
{
    out.assign(n, 0u);

    std::span<const std::byte> in_span(in.data(), in.size());
    std::span<uint32_t> out_span(out.data(), out.size());

    const size_t used = Portable::decode(in_span, n, bit, out_span);
    EXPECT_EQ(used, expectedCompressedBytes(n, bit)) << "Portable decode consumed-bytes must match the format formula";
    EXPECT_EQ(in_span.size(), 0u) << "Portable decode must consume the entire payload span";
    return used;
}

#if USE_SIMDCOMP
static uint32_t encodeSIMDComp(const std::vector<uint32_t> & in, uint32_t bit, std::vector<std::byte> & out)
{
    const size_t expected_bytes = expectedCompressedBytes(in.size(), bit);
    out.assign(expected_bytes, std::byte{0});

    std::span<uint32_t> in_span(const_cast<uint32_t*>(in.data()), in.size());
    std::span<char> out_span(reinterpret_cast<char*>(out.data()), out.size());

    const uint32_t used = SIMDComp::encode(in_span, bit, out_span);
    EXPECT_EQ(size_t(used), expected_bytes) << "Portable encode used-bytes must match the format formula";
    return used;
}

// Portable decode helper: decodes `n` integers from `in` (expected payload length) into `out`.
static size_t decodeSIMDComp(const std::vector<std::byte> & in, size_t n, uint32_t bit, std::vector<uint32_t> & out)
{
    out.assign(n, 0u);

    std::span<const std::byte> in_span(in.data(), in.size());
    std::span<uint32_t> out_span(out.data(), out.size());

    const size_t used = SIMDComp::decode(in_span, n, bit, out_span);
    EXPECT_EQ(used, expectedCompressedBytes(n, bit)) << "Portable decode consumed-bytes must match the format formula";
    EXPECT_EQ(in_span.size(), 0u) << "Portable decode must consume the entire payload span";
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

            const size_t expected_bytes = expectedCompressedBytes(n, bit);

            std::vector<std::byte> enc_port(expected_bytes, std::byte{0});
            std::vector<std::byte> enc_sse(expected_bytes, std::byte{0});

            const uint32_t used_port = encodePortable(input, bit, enc_port);
            const uint32_t used_sse = encodeSIMDComp(input, bit, enc_sse);

            ASSERT_EQ(size_t(used_sse), expected_bytes) << "SSE encode used-bytes mismatch the format formula";
            ASSERT_EQ(size_t(used_port), expected_bytes);

            ASSERT_EQ(enc_port.size(), enc_sse.size());
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

            const size_t expected_bytes = expectedCompressedBytes(n, bit);

            // SSE encode -> Portable decode
            std::vector<std::byte> enc_sse(expected_bytes, std::byte{0});
            const uint32_t used_sse = encodeSIMDComp(input, bit, enc_sse);
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
        const size_t expected_bytes = expectedCompressedBytes(c.n, c.bit);

        for (int pat = 0; pat < 3; ++pat)
        {
            std::vector<uint32_t> input;
            if (pat == 0) input = makeAllZeros(c.n);
            if (pat == 1) input = makeAllMax(c.n, c.bit);
            if (pat == 2) input = makeRandom(c.n, c.bit, 2025u + uint32_t(c.n) + c.bit);

            std::vector<std::byte> enc_port, enc_sse(expected_bytes, std::byte{0});

            encodePortable(input, c.bit, enc_port);
            const uint32_t used_sse = encodeSIMDComp(input, c.bit, enc_sse);
            ASSERT_EQ(size_t(used_sse), expected_bytes);

            ASSERT_EQ(enc_port.size(), expected_bytes);
            ASSERT_EQ(enc_sse.size(), expected_bytes);

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
            const size_t expected_bytes = expectedCompressedBytes(n, bit);

            // -----------------------------------------------------------------
            // Direction 1: Portable encode -> SSE decode
            // -----------------------------------------------------------------
            std::vector<std::byte> enc_port;
            encodePortable(input, bit, enc_port);
            ASSERT_EQ(enc_port.size(), expected_bytes);

            std::vector<uint32_t> dec_sse(n, 0u);
            std::span<const std::byte> enc_port_span(reinterpret_cast<const std::byte*>(enc_port.data()), enc_port.size());
            std::span<uint32_t> dec_sse_span(dec_sse.data(), dec_sse.size());
            const size_t used_dec_sse = SIMDComp::decode(enc_port_span, n, bit, dec_sse_span);
            ASSERT_EQ(used_dec_sse, expected_bytes) << "SSE decode consumed-bytes mismatch (portable payload)";
            ASSERT_EQ(input, dec_sse) << "Portable-encoded payload did not decode correctly in SSE path"
                    << " (n=" << n << ", bit=" << bit << ")";

            // -----------------------------------------------------------------
            // Direction 2: SSE encode -> Portable decode
            // -----------------------------------------------------------------
            std::vector<std::byte> enc_sse(expected_bytes, std::byte{0});
            std::span<uint32_t> input_span(input.data(), input.size());
            std::span<char> enc_sse_span(reinterpret_cast<char*>(enc_sse.data()), enc_sse.size());
            const uint32_t used_enc_sse = SIMDComp::encode(input_span, bit, enc_sse_span);
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
