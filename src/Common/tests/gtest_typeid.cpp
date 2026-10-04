#include <Common/TypeID.h>

#include <string>
#include <string_view>

#include <gtest/gtest.h>

using namespace std::string_view_literals;

namespace DB
{

static std::string encodeSuffix(UInt64 high_bytes, UInt64 low_bytes)
{
    char buf[TYPE_ID_SUFFIX_LENGTH];
    encodeTypeIDSuffix(high_bytes, low_bytes, buf);
    return std::string(buf, TYPE_ID_SUFFIX_LENGTH);
}

struct TestData
{
    int id = 0;
    UInt64 high_bytes = 0; /// first 8 bytes of the UUID, big-endian
    UInt64 low_bytes = 0; /// last 8 bytes of the UUID, big-endian
    std::string_view suffix;
};

/// Suffixes from the official spec test data (spec/valid.yml).
// clang-format off
static const TestData test_data[] =
{
    {1, 0x0000000000000000ULL, 0x0000000000000000ULL, "00000000000000000000000000"sv},
    {2, 0x0000000000000000ULL, 0x0000000000000001ULL, "00000000000000000000000001"sv},
    {3, 0x0000000000000000ULL, 0x000000000000000aULL, "0000000000000000000000000a"sv},
    {4, 0x0000000000000000ULL, 0x0000000000000010ULL, "0000000000000000000000000g"sv},
    {5, 0x0000000000000000ULL, 0x0000000000000020ULL, "00000000000000000000000010"sv},
    {6, 0xffffffffffffffffULL, 0xffffffffffffffffULL, "7zzzzzzzzzzzzzzzzzzzzzzzzz"sv},
    {7, 0x0110c8531d0952d8ULL, 0xd73e1194e95b5f19ULL, "0123456789abcdefghjkmnpqrs"sv},
    {8, 0x01890a5dac96774bULL, 0xbcceb302099a8057ULL, "01h455vb4pex5vsknk084sn02q"sv},
};
// clang-format on

TEST(TypeID, EncodeDecodeSuffix)
{
    for (const auto & v : test_data)
    {
        EXPECT_EQ(encodeSuffix(v.high_bytes, v.low_bytes), v.suffix) << "id=" << v.id;

        UInt64 high_bytes = 0;
        UInt64 low_bytes = 0;
        ASSERT_TRUE(decodeTypeIDSuffix(v.suffix, high_bytes, low_bytes)) << "id=" << v.id;
        EXPECT_EQ(high_bytes, v.high_bytes) << "id=" << v.id;
        EXPECT_EQ(low_bytes, v.low_bytes) << "id=" << v.id;
    }
}

TEST(TypeID, DecodeInvalidSuffix)
{
    UInt64 high_bytes = 0;
    UInt64 low_bytes = 0;

    for (std::string_view bad : {
             ""sv,
             "1234567890123456789012345"sv, /// 25 characters
             "123456789012345678901234567"sv, /// 27 characters
             "0123456789ABCDEFGHJKMNPQRS"sv, /// uppercase
             "ooooooiiiiiiuuuuuuulllllll"sv, /// ambiguous Crockford characters
             "i23456789ol23456789oi23456"sv,
             "123456789-123456789-123456"sv, /// hyphens
             "8zzzzzzzzzzzzzzzzzzzzzzzzz"sv, /// overflows 128 bits
             "1234567890123456789012345 "sv, /// space
         })
    {
        EXPECT_FALSE(decodeTypeIDSuffix(bad, high_bytes, low_bytes)) << bad;
    }
}

TEST(TypeID, PrefixValidation)
{
    for (std::string_view good : {
             ""sv,
             "a"sv,
             "prefix"sv,
             "pre_fix"sv,
             "pre__fix"sv, /// consecutive underscores in the middle are allowed
             "abcdefghijklmnopqrstuvwxyz"sv,
             "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijk"sv, /// 63 characters
         })
    {
        EXPECT_TRUE(isValidTypeIDPrefix(good)) << good;
    }

    for (std::string_view bad : {
             "PREFIX"sv,
             "12345"sv,
             "pre.fix"sv,
             "pre fix"sv,
             "_prefix"sv,
             "prefix_"sv,
             "_"sv,
             "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijkl"sv, /// 64 characters
         })
    {
        EXPECT_FALSE(isValidTypeIDPrefix(bad)) << bad;
    }
}

TEST(TypeID, Split)
{
    std::string_view prefix;
    std::string_view suffix;

    ASSERT_TRUE(splitTypeID("prefix_01h455vb4pex5vsknk084sn02q"sv, prefix, suffix));
    EXPECT_EQ(prefix, "prefix"sv);
    EXPECT_EQ(suffix, "01h455vb4pex5vsknk084sn02q"sv);

    ASSERT_TRUE(splitTypeID("pre_fix_00000000000000000000000000"sv, prefix, suffix));
    EXPECT_EQ(prefix, "pre_fix"sv);
    EXPECT_EQ(suffix, "00000000000000000000000000"sv);

    ASSERT_TRUE(splitTypeID("00000000000000000000000000"sv, prefix, suffix));
    EXPECT_EQ(prefix, ""sv);
    EXPECT_EQ(suffix, "00000000000000000000000000"sv);

    /// Malformed structure (spec/invalid.yml).
    for (std::string_view bad : {
             ""sv,
             "_"sv,
             "_00000000000000000000000000"sv, /// separator with the empty prefix
             "_prefix_00000000000000000000000000"sv,
             "prefix__00000000000000000000000000"sv, /// prefix ends with an underscore
             "prefix_"sv,
             "prefix_1234567890123456789012345"sv, /// short suffix
             "prefix_123456789012345678901234567"sv, /// long suffix
             "PREFIX_00000000000000000000000000"sv,
             "12345_00000000000000000000000000"sv,
             "pre.fix_00000000000000000000000000"sv,
             "  prefix_00000000000000000000000000"sv,
         })
    {
        EXPECT_FALSE(splitTypeID(bad, prefix, suffix)) << bad;
    }
}

} // namespace DB
