#include <gtest/gtest.h>

#include <random>

#include <Common/BitPackedStringArray.h>

using namespace DB;

namespace
{

void checkRoundTrip(const std::vector<std::string> & strings)
{
    /// Concatenated chars and end offsets, the same layout as in ColumnString.
    std::string chars;
    std::vector<UInt64> offsets;
    offsets.reserve(strings.size());

    for (const auto & str : strings)
    {
        chars += str;
        offsets.push_back(chars.size());
    }

    BitPackedStringArray packed{std::span(chars.data(), chars.size()), std::span(offsets.data(), offsets.size())};

    ASSERT_EQ(packed.size(), strings.size());
    for (size_t i = 0; i < strings.size(); ++i)
        ASSERT_EQ(packed.get(i), strings[i]) << "at index " << i;
}

}

TEST(BitPackedStringArray, Empty)
{
    BitPackedStringArray packed;
    EXPECT_EQ(packed.size(), 0u);
    EXPECT_TRUE(packed.empty());

    BitPackedStringArray packed_from_empty{std::span<const char>{}, std::span<const UInt64>{}};
    EXPECT_EQ(packed_from_empty.size(), 0u);
    EXPECT_TRUE(packed_from_empty.empty());
}

TEST(BitPackedStringArray, FromPODArrays)
{
    PaddedPODArray<UInt8> chars;
    PaddedPODArray<UInt64> offsets;

    for (std::string_view str : {"ab", "", "xyz"})
    {
        chars.insert(str.begin(), str.end());
        offsets.push_back(chars.size());
    }

    BitPackedStringArray packed{chars, offsets};

    ASSERT_EQ(packed.size(), 3u);
    EXPECT_EQ(packed.get(0), "ab");
    EXPECT_EQ(packed.get(1), "");
    EXPECT_EQ(packed.get(2), "xyz");
}

TEST(BitPackedStringArray, SingleString)
{
    checkRoundTrip({""});
    checkRoundTrip({"a"});
    checkRoundTrip({"hello world"});
}

TEST(BitPackedStringArray, EmptyStrings)
{
    checkRoundTrip({"", "", ""});
    checkRoundTrip({"", "abc", "", "def", ""});
}

TEST(BitPackedStringArray, BinaryData)
{
    checkRoundTrip({std::string("\x00\x01\x02", 3), std::string("a\x00""b", 3)});
}

TEST(BitPackedStringArray, BlockBoundaries)
{
    std::vector<std::string> strings;
    for (size_t size : {127, 128, 129, 256, 257})
    {
        strings.clear();
        for (size_t i = 0; i < size; ++i)
            strings.push_back("token" + std::to_string(i));

        checkRoundTrip(strings);
    }
}

TEST(BitPackedStringArray, RandomStrings)
{
    std::mt19937_64 rng(42); // NOLINT(cert-msc32-c,cert-msc51-cpp): deterministic seed for reproducible test
    std::vector<std::string> strings;

    for (size_t i = 0; i < 10000; ++i)
    {
        std::string str(rng() % 30, ' ');
        for (auto & c : str)
            c = static_cast<char>('a' + rng() % 26);
        strings.push_back(std::move(str));
    }

    checkRoundTrip(strings);
}
