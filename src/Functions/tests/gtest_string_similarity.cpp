#include <gtest/gtest.h>

#include <array>
#include <iomanip>
#include <random>
#include <sstream>
#include <string_view>

#define STRING_SIMILARITY_GTEST_UNIT_TEST
#include "Functions/FunctionsStringDistance.cpp" // NOLINT(bugprone-suspicious-include)

using namespace DB;

template <bool IsUtf8>
using SymbolT = std::conditional_t<IsUtf8, UInt32, UInt8>;

template <bool is_utf8>
UInt32 referenceDistance(const SymbolT<is_utf8> * a, size_t a_size, const SymbolT<is_utf8> * b, size_t b_size)
{
    return ByteEditDistanceDPImpl<is_utf8>::distance(a, static_cast<UInt32>(a_size), b, static_cast<UInt32>(b_size));
}

template <bool is_utf8>
UInt32 myersDistance(const SymbolT<is_utf8> * haystack, size_t haystack_size, const SymbolT<is_utf8> * needle, size_t needle_size)
{
    if (needle_size <= 64)
        return ByteEditDistanceMyersImpl<is_utf8, UInt64>::distance(
            haystack, static_cast<UInt32>(haystack_size), needle, static_cast<UInt32>(needle_size));
    if (needle_size <= 128)
        return ByteEditDistanceMyersImpl<is_utf8, UInt128>::distance(
            haystack, static_cast<UInt32>(haystack_size), needle, static_cast<UInt32>(needle_size));
    if (needle_size <= 256)
        return ByteEditDistanceMyersImpl<is_utf8, UInt256>::distance(
            haystack, static_cast<UInt32>(haystack_size), needle, static_cast<UInt32>(needle_size));

    return BlockMyersEditDistance<is_utf8>::distance(
        haystack, static_cast<UInt32>(haystack_size), needle, static_cast<UInt32>(needle_size));
}

static VectorWithMemoryTracking<UInt32> toUTF8CodePoints(std::u8string_view s)
{
    VectorWithMemoryTracking<UInt32> out;
    parseUTF8String(reinterpret_cast<const char *>(s.data()), s.size(), [&](UInt32 cp) { out.push_back(cp); });
    return out;
}

static void runTest(std::string_view a, std::string_view b)
{
    if (a.empty() || b.empty())
        return;

    std::string_view haystack = (a.size() >= b.size()) ? a : b;
    std::string_view needle = (a.size() >= b.size()) ? b : a;

    UInt32 expected = referenceDistance<false>(
        reinterpret_cast<const UInt8 *>(haystack.data()), haystack.size(), reinterpret_cast<const UInt8 *>(needle.data()), needle.size());
    UInt32 actual = myersDistance<false>(
        reinterpret_cast<const UInt8 *>(haystack.data()), haystack.size(), reinterpret_cast<const UInt8 *>(needle.data()), needle.size());

    ASSERT_EQ(expected, actual) << "haystack: [" << haystack << "]\n" << "needle: [" << needle << "]";
}

static void runTestUTF8(std::u8string_view a_raw, std::u8string_view b_raw)
{
    auto a = toUTF8CodePoints(a_raw);
    auto b = toUTF8CodePoints(b_raw);

    if (a.empty() || b.empty())
        return;

    const auto & haystack = (a.size() >= b.size()) ? a : b;
    const auto & needle = (a.size() >= b.size()) ? b : a;

    UInt32 expected = referenceDistance<true>(haystack.data(), haystack.size(), needle.data(), needle.size());

    UInt32 actual = myersDistance<true>(haystack.data(), haystack.size(), needle.data(), needle.size());

    ASSERT_EQ(expected, actual);
}

TEST(StringSimilarity, EditDistanceBasicCases)
{
    runTest("abc", "abc");
    runTest("abc", "abd");
    runTest("kitten", "sitting");
    runTest("saturday", "sunday");
    runTest("abcdef", "azced");
    runTestUTF8(u8"Łódź", u8"Łódź");
    runTestUTF8(u8"Łódź Kaliska", u8"Ućka Liska");
    runTestUTF8(u8"żółw", u8"żołw");
    runTestUTF8(u8"kot", u8"kót");
    runTestUTF8(u8"你好", u8"你号");
    runTestUTF8(u8"🙂🙃🙂", u8"🙂🙂🙂");
}


class EditDistanceParamTest : public ::testing::TestWithParam<size_t>
{
};

TEST_P(EditDistanceParamTest, EditDistanceMultiBlockAscii)
{
    const size_t b_len = GetParam();
    const size_t a_len = b_len + 30;

    std::string long_a(a_len, 'a');
    std::string long_b(b_len, 'a');

    if (b_len > 0)
        long_b[0] = 'z';

    if (b_len > 1)
        long_b[b_len - 1] = 'y';

    if (b_len > 64)
        long_b[63] = 'b';

    if (b_len > 65)
        long_b[64] = 'c';

    if (b_len > 128)
        long_b[127] = 'd';

    if (b_len > 129)
        long_b[128] = 'e';

    if (b_len > 256)
        long_b[b_len / 2] = 'f';

    if (b_len > 512)
        long_b[(3 * b_len) / 4] = 'g';

    runTest(long_a, long_b);


    {
        const std::string base = "AGTACGCATGCTAGTCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCG";

        std::string s1;
        while (s1.size() < b_len)
            s1 += base;

        s1.resize(b_len);

        std::string s2 = s1;

        if (b_len > 10)
            s2[10] ^= 1;

        if (b_len > 63)
            s2[63] ^= 1;

        if (b_len > 64)
            s2[64] ^= 1;

        if (b_len > 127)
            s2[127] ^= 1;

        if (b_len > 255)
            s2[255] ^= 1;

        if (b_len > 511)
            s2[511] ^= 1;

        if (b_len > 0)
            s2[b_len - 1] ^= 1;

        runTest(s1, s2);
    }
    {
        std::string s1(b_len, 'a');
        std::string s2(b_len, 'b');

        if (b_len > 0)
        {
            // Make a few positions equal to avoid trivial case
            s2[b_len / 3] = 'a';
            s2[(2 * b_len) / 3] = 'a';
        }

        runTest(s1, s2);
    }
}

TEST_P(EditDistanceParamTest, EditDistanceMultiBlockUtf8)
{
    const size_t b_len = GetParam();
    const size_t a_len = b_len + 30;

    VectorWithMemoryTracking<UInt32> long_a_codepoints;
    long_a_codepoints.reserve(a_len);
    for (size_t i = 0; i < a_len; ++i)
        long_a_codepoints.push_back(U'Ł');

    VectorWithMemoryTracking<UInt32> long_b_codepoints;
    long_b_codepoints.reserve(b_len);
    for (size_t i = 0; i < b_len; ++i)
        long_b_codepoints.push_back(U'漢');

    if (b_len > 0)
        long_b_codepoints[0] = U'한';

    if (b_len > 1)
        long_b_codepoints[b_len - 1] = U'ზ';

    if (b_len > 64)
        long_b_codepoints[63] = U'ѭ';

    if (b_len > 65)
        long_b_codepoints[64] = U'ę';

    if (b_len > 128)
        long_b_codepoints[127] = U'ł';

    if (b_len > 129)
        long_b_codepoints[128] = U'ń';

    if (b_len > 256)
        long_b_codepoints[b_len / 2] = U'ś';

    if (b_len > 512)
        long_b_codepoints[(3 * b_len) / 4] = U'ź';

    UInt32 expected
        = referenceDistance<true>(long_a_codepoints.data(), long_a_codepoints.size(), long_b_codepoints.data(), long_b_codepoints.size());

    UInt32 actual
        = myersDistance<true>(long_a_codepoints.data(), long_a_codepoints.size(), long_b_codepoints.data(), long_b_codepoints.size());

    ASSERT_EQ(expected, actual);


    {
        const VectorWithMemoryTracking<UInt32> base_codepoints = []
        {
            VectorWithMemoryTracking<UInt32> result;
            std::u8string base = u8"ŻółćĄĆĘŁŃÓŚŹ😀🐍漢字";
            parseUTF8String(reinterpret_cast<const char *>(base.data()), base.size(), [&](UInt32 cp) { result.push_back(cp); });
            return result;
        }();

        VectorWithMemoryTracking<UInt32> s1_codepoints;
        while (s1_codepoints.size() < b_len)
            s1_codepoints.insert(s1_codepoints.end(), base_codepoints.begin(), base_codepoints.end());
        s1_codepoints.resize(b_len, U' ');

        VectorWithMemoryTracking<UInt32> s2_codepoints = s1_codepoints;

        if (b_len > 10)
            s2_codepoints[10] = 0x0179; // Ź

        if (b_len > 63)
            s2_codepoints[63] = 0x1F40D; // 🐍

        if (b_len > 127)
            s2_codepoints[127] = 0x5B57; // 字

        if (b_len > 0)
            s2_codepoints[b_len - 1] = 0x015A; // Ś

        expected = referenceDistance<true>(s1_codepoints.data(), s1_codepoints.size(), s2_codepoints.data(), s2_codepoints.size());

        actual = myersDistance<true>(s1_codepoints.data(), s1_codepoints.size(), s2_codepoints.data(), s2_codepoints.size());

        ASSERT_EQ(expected, actual);
    }
    {
        VectorWithMemoryTracking<UInt32> s1(b_len, U'漢');
        VectorWithMemoryTracking<UInt32> s2(b_len, U'😀');

        if (b_len > 0)
        {
            s2[b_len / 3] = U'漢';
            s2[(2 * b_len) / 3] = U'漢';
        }

        expected = referenceDistance<true>(s1.data(), s1.size(), s2.data(), s2.size());

        actual = myersDistance<true>(s1.data(), s1.size(), s2.data(), s2.size());

        ASSERT_EQ(expected, actual);
    }
}

INSTANTIATE_TEST_SUITE_P(
    StringSimilarity,
    EditDistanceParamTest,
    ::testing::Values(1, 2, 63, 64, 65, 127, 128, 129, 255, 256, 257, 511, 512, 513, 1023, 1024, 1025, 4095, 4096, 4097, 8191, 8192, 8193));


static std::string toHex(const UInt8 * data, size_t size)
{
    std::ostringstream oss;
    oss << std::hex << std::setfill('0');
    for (size_t i = 0; i < size; ++i)
        oss << std::setw(2) << static_cast<unsigned>(data[i]);
    return oss.str();
}

static std::string toHex(const UInt32 * data, size_t size)
{
    std::ostringstream oss;
    oss << std::hex << std::setfill('0');
    for (size_t i = 0; i < size; ++i)
        oss << std::setw(8) << data[i];
    return oss.str();
}

TEST(StringSimilarity, EditDistanceStressAscii)
{
    std::random_device rd;
    std::mt19937_64 rng(rd());
    std::uniform_int_distribution<size_t> len_dist(1, 8193);
    std::uniform_int_distribution<int> char_dist(0, 255);

    constexpr size_t iterations = 200;

    for (size_t it = 0; it < iterations; ++it)
    {
        size_t len_a = len_dist(rng);
        size_t len_b = len_dist(rng);

        VectorWithMemoryTracking<UInt8> a(len_a);
        VectorWithMemoryTracking<UInt8> b(len_b);

        for (auto & c : a)
            c = static_cast<UInt8>(char_dist(rng));
        for (auto & c : b)
            c = static_cast<UInt8>(char_dist(rng));

        UInt32 ref = referenceDistance<false>(a.data(), a.size(), b.data(), b.size());

        UInt32 my = myersDistance<false>(a.data(), a.size(), b.data(), b.size());

        if (ref != my)
        {
            FAIL() << "\nASCII stress test failed\n"
                   << "a(hex): " << toHex(a.data(), a.size()) << "\n"
                   << "b(hex): " << toHex(b.data(), b.size()) << "\n"
                   << "ref=" << ref << " my=" << my;
        }
    }
}

TEST(StringSimilarity, EditDistanceStressUtf8)
{
    std::random_device rd;
    std::mt19937_64 rng(rd());
    std::uniform_int_distribution<size_t> len_dist(1, 8193);
    std::uniform_int_distribution<UInt32> cp_dist(0, 0x10FFFF);

    constexpr size_t iterations = 200;

    auto randomCodePoint = [&]
    {
        while (true)
        {
            UInt32 cp = cp_dist(rng);
            if (cp >= 0xD800 && cp <= 0xDFFF)
                continue; // skip surrogate range
            return cp;
        }
    };

    for (size_t it = 0; it < iterations; ++it)
    {
        size_t len_a = len_dist(rng);
        size_t len_b = len_dist(rng);

        VectorWithMemoryTracking<UInt32> a(len_a);
        VectorWithMemoryTracking<UInt32> b(len_b);

        for (auto & cp : a)
            cp = randomCodePoint();
        for (auto & cp : b)
            cp = randomCodePoint();

        UInt32 ref = referenceDistance<true>(a.data(), a.size(), b.data(), b.size());

        UInt32 my = myersDistance<true>(a.data(), a.size(), b.data(), b.size());

        if (ref != my)
        {
            FAIL() << "\nUTF8 stress test failed\n"
                   << "a(hex): " << toHex(a.data(), a.size()) << "\n"
                   << "b(hex): " << toHex(b.data(), b.size()) << "\n"
                   << "ref=" << ref << " my=" << my;
        }
    }
}


/// Longest common subsequence: the bit-parallel implementation against textbook dynamic programming.

template <bool is_utf8>
UInt32 referenceLCS(const SymbolT<is_utf8> * a, size_t a_size, const SymbolT<is_utf8> * b, size_t b_size)
{
    VectorWithMemoryTracking<UInt32> prev(b_size + 1, 0);
    VectorWithMemoryTracking<UInt32> curr(b_size + 1, 0);
    for (size_t i = 1; i <= a_size; ++i)
    {
        for (size_t j = 1; j <= b_size; ++j)
            curr[j] = (a[i - 1] == b[j - 1]) ? prev[j - 1] + 1 : std::max(prev[j], curr[j - 1]);
        std::swap(prev, curr);
    }
    return prev[b_size];
}

template <bool is_utf8>
UInt32 bitParallelLCS(const SymbolT<is_utf8> * a, size_t a_size, const SymbolT<is_utf8> * b, size_t b_size)
{
    /// The bit vector runs over the shorter string, which must not be empty.
    if (a_size < b_size)
    {
        std::swap(a, b);
        std::swap(a_size, b_size);
    }
    if (b_size == 0)
        return 0;
    return LongestCommonSubsequenceImpl<is_utf8>::length(a, static_cast<UInt32>(a_size), b, static_cast<UInt32>(b_size));
}

static void runLCSTest(std::string_view a, std::string_view b)
{
    const auto * a_data = reinterpret_cast<const UInt8 *>(a.data());
    const auto * b_data = reinterpret_cast<const UInt8 *>(b.data());
    UInt32 expected = referenceLCS<false>(a_data, a.size(), b_data, b.size());
    UInt32 actual = bitParallelLCS<false>(a_data, a.size(), b_data, b.size());
    ASSERT_EQ(expected, actual) << "a: [" << a << "]\nb: [" << b << "]";
}

class LongestCommonSubsequenceParamTest : public ::testing::TestWithParam<size_t>
{
};

/// Lengths around word boundaries, where the carry of the addition crosses from one word to the next.
TEST_P(LongestCommonSubsequenceParamTest, LongestCommonSubsequenceMultiBlock)
{
    const size_t len = GetParam();

    /// Periodic strings shifted by one symbol: "abab..." vs "baba...", LCS = len - 1.
    std::string periodic_a;
    std::string periodic_b;
    for (size_t i = 0; i < len; ++i)
    {
        periodic_a += "ab"[i % 2];
        periodic_b += "ba"[i % 2];
    }
    runLCSTest(periodic_a, periodic_b);

    /// One mismatching symbol at the end of each word.
    std::string same(len + 30, 'a');
    std::string changed(len, 'a');
    for (size_t i = 63; i < len; i += 64)
        changed[i] = 'b';
    runLCSTest(same, changed);

    /// Random strings over small alphabets, which have long common subsequences.
    std::mt19937_64 rng(len);
    for (size_t alphabet : {2, 4, 26})
    {
        std::uniform_int_distribution<int> char_dist(0, static_cast<int>(alphabet) - 1);
        std::string a(len + 17, ' ');
        std::string b(len, ' ');
        for (auto & c : a)
            c = static_cast<char>('a' + char_dist(rng));
        for (auto & c : b)
            c = static_cast<char>('a' + char_dist(rng));
        runLCSTest(a, b);
    }
}

INSTANTIATE_TEST_SUITE_P(
    StringSimilarity,
    LongestCommonSubsequenceParamTest,
    ::testing::Values(1, 2, 63, 64, 65, 127, 128, 129, 191, 192, 193, 255, 256, 257, 511, 512, 513, 1023, 1024, 1025, 2049));

TEST(StringSimilarity, LongestCommonSubsequenceStress)
{
    std::random_device rd;
    std::mt19937_64 rng(rd());
    std::uniform_int_distribution<size_t> len_dist(0, 1500);
    constexpr std::array<UInt32, 4> alphabets = {2, 4, 26, 256};
    std::uniform_int_distribution<size_t> alphabet_dist(0, alphabets.size() - 1);

    constexpr size_t iterations = 100;

    for (size_t it = 0; it < iterations; ++it)
    {
        const UInt32 alphabet = alphabets[alphabet_dist(rng)];
        std::uniform_int_distribution<UInt32> symbol_dist(0, alphabet - 1);

        VectorWithMemoryTracking<UInt8> a(len_dist(rng));
        VectorWithMemoryTracking<UInt8> b(len_dist(rng));
        for (auto & c : a)
            c = static_cast<UInt8>(symbol_dist(rng));
        for (auto & c : b)
            c = static_cast<UInt8>(symbol_dist(rng));

        UInt32 ref = referenceLCS<false>(a.data(), a.size(), b.data(), b.size());
        UInt32 my = bitParallelLCS<false>(a.data(), a.size(), b.data(), b.size());
        if (ref != my)
        {
            FAIL() << "\nLCS ASCII stress test failed\n"
                   << "a(hex): " << toHex(a.data(), a.size()) << "\n"
                   << "b(hex): " << toHex(b.data(), b.size()) << "\n"
                   << "ref=" << ref << " my=" << my;
        }

        /// The same strings as code points, spread over the whole Unicode range.
        VectorWithMemoryTracking<UInt32> a_utf8(a.begin(), a.end());
        VectorWithMemoryTracking<UInt32> b_utf8(b.begin(), b.end());
        for (auto & cp : a_utf8)
            cp = cp * 4099 + 0x80;
        for (auto & cp : b_utf8)
            cp = cp * 4099 + 0x80;

        ref = referenceLCS<true>(a_utf8.data(), a_utf8.size(), b_utf8.data(), b_utf8.size());
        my = bitParallelLCS<true>(a_utf8.data(), a_utf8.size(), b_utf8.data(), b_utf8.size());
        if (ref != my)
        {
            FAIL() << "\nLCS UTF8 stress test failed\n"
                   << "a(hex): " << toHex(a_utf8.data(), a_utf8.size()) << "\n"
                   << "b(hex): " << toHex(b_utf8.data(), b_utf8.size()) << "\n"
                   << "ref=" << ref << " my=" << my;
        }
    }
}
