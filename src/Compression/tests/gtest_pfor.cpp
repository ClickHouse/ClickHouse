#include <Compression/PFor.h>

#include <cstdint>
#include <cstring>
#include <limits>
#include <random>
#include <span>
#include <vector>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

/// Round-trips `in` through the headerless block API (what the text index uses) and checks:
///  - exact round-trip,
///  - exact bounded consumption (decode reports exactly what encode produced),
///  - deterministic encoding: re-encoding into two differently-poisoned buffers must yield
///    byte-identical output, i.e. no uninitialized slack bits.
template <typename T>
void checkBlocksRoundTrip(const std::vector<T> & in, PFor::Delta mode)
{
    const size_t n = in.size();
    const size_t bound = PFor::maxCompressedBytes<T>(n);

    std::vector<uint8_t> a(bound + 64, 0xAA);
    std::vector<uint8_t> b(bound + 64, 0x55);
    const size_t sa = PFor::encodeBlocks<T>(std::span<const T>(in), mode, a.data());
    const size_t sb = PFor::encodeBlocks<T>(std::span<const T>(in), mode, b.data());

    ASSERT_LE(sa, bound) << "encoded size exceeds maxCompressedBytes (n=" << n << ")";
    ASSERT_EQ(sa, sb) << "encoded size must not depend on buffer contents (n=" << n << ")";
    EXPECT_EQ(std::memcmp(a.data(), b.data(), sa), 0) << "encoding must be deterministic (n=" << n << ")";

    std::vector<T> decoded(n + 64, 0);
    const size_t consumed = PFor::decodeBlocks<T>(a.data(), n, mode, decoded.data());

    EXPECT_EQ(consumed, sa) << "decode must consume exactly what encode produced (n=" << n << ")";
    for (size_t i = 0; i < n; ++i)
        ASSERT_EQ(decoded[i], in[i]) << "mismatch at " << i << " of " << n;
}

/// Round-trips through the self-describing buffer API (varint count + flags + block stream).
template <typename T>
void checkSelfDescribingRoundTrip(const std::vector<T> & in, PFor::Delta mode)
{
    std::vector<uint8_t> compressed = PFor::compress<T>(std::span<const T>(in), mode);
    compressed.resize(compressed.size() + 64, 0); /// padding guard for the SIMD loads

    EXPECT_EQ(PFor::decompressedCount(std::span<const uint8_t>(compressed)), in.size());

    std::vector<T> decoded = PFor::decompress<T>(std::span<const uint8_t>(compressed));
    EXPECT_EQ(decoded, in);
}

/// Exercises both the headerless and the self-describing APIs.
template <typename T>
void checkRoundTrip(const std::vector<T> & in, PFor::Delta mode)
{
    checkBlocksRoundTrip<T>(in, mode);
    checkSelfDescribingRoundTrip<T>(in, mode);
}

/// Inclusive prefix sum of `gaps` (turns gaps into a non-decreasing value stream).
template <typename T>
std::vector<T> cumulative(const std::vector<T> & gaps)
{
    std::vector<T> v(gaps.size());
    T acc = 0;
    for (size_t i = 0; i < gaps.size(); ++i)
    {
        acc = static_cast<T>(acc + gaps[i]);
        v[i] = acc;
    }
    return v;
}

/// Sizes around block boundaries (BLOCK = 128): empty, sub-block, exact, tail, multi-block.
const std::vector<size_t> & testSizes()
{
    static const std::vector<size_t> sizes = {0, 1, 2, 63, 64, 65, 127, 128, 129, 200, 255, 256, 257, 512, 1000, 5000};
    return sizes;
}

}

template <typename T>
class PForRoundTrip : public ::testing::Test
{
};

using ElementTypes = ::testing::Types<uint32_t, uint64_t>;
TYPED_TEST_SUITE(PForRoundTrip, ElementTypes);

/// Random values at every block boundary (no delta): base-width selection and partial tails.
TYPED_TEST(PForRoundTrip, RandomBoundaries)
{
    using T = TypeParam;
    std::mt19937_64 rng(0xC0FFEE);
    for (size_t n : testSizes())
    {
        std::vector<T> v(n);
        for (auto & x : v)
            x = static_cast<T>(rng());
        checkRoundTrip<T>(v, PFor::Delta::none);
    }
}

/// Constant / zero blocks, including a constant at every byte-width boundary (value needs k*8 bits).
TYPED_TEST(PForRoundTrip, ConstantBlocks)
{
    using T = TypeParam;
    for (size_t n : testSizes())
    {
        checkRoundTrip<T>(std::vector<T>(n, T{0}), PFor::Delta::none);
        checkRoundTrip<T>(std::vector<T>(n, std::numeric_limits<T>::max()), PFor::Delta::none);
        checkRoundTrip<T>(std::vector<T>(n, T{42}), PFor::Delta::none);
    }
    for (unsigned k = 1; k <= sizeof(T); ++k)
    {
        const T v = static_cast<T>((~T(0)) >> (8 * (sizeof(T) - k)));
        checkRoundTrip<T>(std::vector<T>(384, v), PFor::Delta::none);
        checkRoundTrip<T>(std::vector<T>(384, static_cast<T>(v >> 1)), PFor::Delta::none);
    }
}

/// Frame-of-reference: high base with a small spread.
TYPED_TEST(PForRoundTrip, ForHighBase)
{
    using T = TypeParam;
    std::mt19937_64 rng(0x1234);
    std::vector<T> v(600);
    std::uniform_int_distribution<uint64_t> d(0, 63);
    for (auto & x : v)
        x = static_cast<T>((sizeof(T) == 8 ? 0x1000000000ULL : 1'000'000ULL) + d(rng));
    checkRoundTrip<T>(v, PFor::Delta::none);
}

/// PFor exception path: mostly tiny values with rare huge outliers.
TYPED_TEST(PForRoundTrip, PForOutliers)
{
    using T = TypeParam;
    std::mt19937_64 rng(0x5678);
    std::vector<T> v(2000);
    std::uniform_int_distribution<uint32_t> small(0, 15);
    for (auto & x : v)
        x = small(rng);
    for (int k = 0; k < 25; ++k)
        v[rng() % v.size()] = static_cast<T>(std::numeric_limits<T>::max() - (rng() % 1000));
    checkRoundTrip<T>(v, PFor::Delta::none);
}

/// Few huge exceptions over a tiny base (vbyte exception mode).
TYPED_TEST(PForRoundTrip, VbyteMode)
{
    using T = TypeParam;
    std::vector<T> v(2000, 3);
    for (int k = 0; k < 6; ++k)
        v[100 + 300 * k] = static_cast<T>(std::numeric_limits<T>::max() - k);
    checkRoundTrip<T>(v, PFor::Delta::none);
}

/// Exceptions placed to hit all 16 per-group nibble masks.
TYPED_TEST(PForRoundTrip, BitmapNibbles)
{
    using T = TypeParam;
    std::vector<T> v(256, 1);
    for (unsigned g = 0; g < 16; ++g)
        for (unsigned k = 0; k < 4; ++k)
            if (g & (1u << k))
                v[g * 4 + k] = static_cast<T>(T(1) << (4 * sizeof(T)));
    checkRoundTrip<T>(v, PFor::Delta::none);
}

/// Delta (non-decreasing): doc-id-like varied gaps with occasional large jumps, plus dense gaps.
TYPED_TEST(PForRoundTrip, DeltaNonDecreasing)
{
    using T = TypeParam;
    std::mt19937_64 rng(0xD0C1D);

    std::vector<T> gaps(10000);
    std::uniform_int_distribution<uint32_t> g(1, 50);
    for (auto & x : gaps)
        x = g(rng);
    for (int k = 0; k < 100; ++k)
        gaps[rng() % gaps.size()] = static_cast<T>(100000 + (rng() % 1000000));
    checkRoundTrip<T>(cumulative<T>(gaps), PFor::Delta::d0);

    std::vector<T> dense(8000);
    std::uniform_int_distribution<uint32_t> g2(0, 2);
    for (auto & x : dense)
        x = g2(rng);
    checkRoundTrip<T>(cumulative<T>(dense), PFor::Delta::d0);

    checkRoundTrip<T>({}, PFor::Delta::d0);
    checkRoundTrip<T>(std::vector<T>{T{123456}}, PFor::Delta::d0);
}

/// Delta (strictly increasing): gap-1 encoding, including all-consecutive (gaps-1 == 0).
TYPED_TEST(PForRoundTrip, DeltaStrictlyIncreasing)
{
    using T = TypeParam;
    std::mt19937_64 rng(0x1AC1D);
    std::vector<T> v(5000);
    std::uniform_int_distribution<uint32_t> g(1, 40);
    T acc = 0;
    for (auto & x : v)
    {
        acc = static_cast<T>(acc + g(rng));
        x = acc;
    }
    checkRoundTrip<T>(v, PFor::Delta::d1);
    checkRoundTrip<T>(cumulative<T>(std::vector<T>(300, T{1})), PFor::Delta::d1);
}

/// First value at varint framing boundaries for the delta stream.
TYPED_TEST(PForRoundTrip, VbxFirstValue)
{
    using T = TypeParam;
    std::mt19937_64 rng(0xFADE);
    for (T first : {T(127), T(128), T(16383), T(16384), T((T(1) << 27) + 1), static_cast<T>(std::numeric_limits<T>::max() / 2)})
    {
        std::vector<T> v(700);
        v[0] = first;
        std::uniform_int_distribution<uint32_t> g(0, 6);
        for (size_t i = 1; i < v.size(); ++i)
            v[i] = static_cast<T>(v[i - 1] + g(rng));
        checkRoundTrip<T>(v, PFor::Delta::d0);
    }
}

/// 64-bit specific: a stream whose running value crosses 2^32 mid-block (the delta carry must
/// stay 64-bit), and full-width b == 64 blocks.
TYPED_TEST(PForRoundTrip, U64CrossBoundaryAndFullWidth)
{
    using T = TypeParam;
    if constexpr (sizeof(T) == 8)
    {
        std::mt19937_64 rng(0xB16B00);
        std::vector<T> v(900);
        T acc = 0xFFFFFF00ull; /// crosses 2^32 within the first block
        std::uniform_int_distribution<uint32_t> g(1, 9);
        for (auto & x : v)
        {
            acc += g(rng);
            x = acc;
        }
        checkRoundTrip<T>(v, PFor::Delta::d0);
        checkRoundTrip<T>(v, PFor::Delta::d1);

        std::vector<T> w(640);
        for (auto & x : w)
            x = rng() | (T(1) << 63); /// forces b = 64
        checkRoundTrip<T>(w, PFor::Delta::none);
    }
}
