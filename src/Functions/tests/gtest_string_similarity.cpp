#include <gtest/gtest.h>

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

std::vector<UInt32> toUTF8CodePoints(std::u8string_view s)
{
    std::vector<UInt32> out;
    parseUTF8String(reinterpret_cast<const char *>(s.data()), s.size(), [&](UInt32 cp) { out.push_back(cp); });
    return out;
}

void runTest(std::string_view a, std::string_view b)
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

void runTestUTF8(std::u8string_view a_raw, std::u8string_view b_raw)
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

    std::vector<UInt32> long_a_codepoints;
    long_a_codepoints.reserve(a_len);
    for (size_t i = 0; i < a_len; ++i)
        long_a_codepoints.push_back(U'Ł');

    std::vector<UInt32> long_b_codepoints;
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
        const std::vector<UInt32> base_codepoints = []
        {
            std::vector<UInt32> result;
            std::u8string base = u8"ŻółćĄĆĘŁŃÓŚŹ😀🐍漢字";
            parseUTF8String(reinterpret_cast<const char *>(base.data()), base.size(), [&](UInt32 cp) { result.push_back(cp); });
            return result;
        }();

        std::vector<UInt32> s1_codepoints;
        while (s1_codepoints.size() < b_len)
            s1_codepoints.insert(s1_codepoints.end(), base_codepoints.begin(), base_codepoints.end());
        s1_codepoints.resize(b_len, U' ');

        std::vector<UInt32> s2_codepoints = s1_codepoints;

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
        std::vector<UInt32> s1(b_len, U'漢');
        std::vector<UInt32> s2(b_len, U'😀');

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


std::string toHex(const UInt8 * data, size_t size)
{
    std::ostringstream oss;
    oss << std::hex << std::setfill('0');
    for (size_t i = 0; i < size; ++i)
        oss << std::setw(2) << static_cast<unsigned>(data[i]);
    return oss.str();
}

std::string toHex(const UInt32 * data, size_t size)
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

        std::vector<UInt8> a(len_a);
        std::vector<UInt8> b(len_b);

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

        std::vector<UInt32> a(len_a);
        std::vector<UInt32> b(len_b);

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
