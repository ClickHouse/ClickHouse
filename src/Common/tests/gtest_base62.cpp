#include <Common/Base62.h>

#include <string>
#include <string_view>

#include <gtest/gtest.h>

using namespace std::string_view_literals;

namespace DB
{

static std::string encode(std::string_view src)
{
    UInt8 buf[128];
    size_t len = encodeBase62(reinterpret_cast<const UInt8 *>(src.data()), src.size(), buf);
    return std::string(reinterpret_cast<const char *>(buf), len);
}

static std::optional<std::string> decode(std::string_view src)
{
    UInt8 buf[128];
    auto len = decodeBase62(reinterpret_cast<const UInt8 *>(src.data()), src.size(), buf);
    if (!len)
        return std::nullopt;
    return std::string(reinterpret_cast<const char *>(buf), *len);
}

struct TestData
{
    int id = 0;
    std::string_view decoded;
    std::string_view encoded; /// base62
};

// clang-format off
static const TestData test_data[] =
{
    {1, ""sv, ""sv},
    {2, "\x00"sv, "0"sv},
    {3, "\x00\x00"sv, "00"sv},
    {4, "f"sv, "1e"sv},
    {5, "fo"sv, "6ox"sv},
    {6, "foo"sv, "SAPP"sv},
    {7, "foob"sv, "1sIyuo"sv},
    {8, "fooba"sv, "7kENWa1"sv},
    {9, "foobar"sv, "VytN8Wjy"sv},
    {10, "Hello world!"sv, "T8dgcjRGuYUueWht"sv},
    {11, "\x00\x01\x02\x03"sv, "0HBL"sv},
    {12, "\x00\x00\xff"sv, "0047"sv},
    {13, "\xff\xff\xff\xff\xff\xff\xff\xff"sv, "LygHa16AHYF"sv},
    {14, "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f"sv, "03aUlTJC7tjlCTQj2uNU3MFagCXG9LRKRcwGkBIDlf"sv},
    {15, "ClickHouse"sv, "1agk8B30gH5Kj7"sv},
    {16, "Encoded"sv, "1RVU3aMpUa"sv},
};
// clang-format on

TEST(Base62, EncodeDecode)
{
    for (const auto & v : test_data)
    {
        EXPECT_EQ(encode(v.decoded), v.encoded) << "id=" << v.id;

        auto decoded = decode(v.encoded);
        ASSERT_TRUE(decoded.has_value()) << "id=" << v.id;
        EXPECT_EQ(*decoded, v.decoded) << "id=" << v.id;
    }
}

TEST(Base62, Roundtrip)
{
    /// Exhaustive roundtrip over all 1-byte and 2-byte inputs.
    for (size_t a = 0; a < 256; ++a)
    {
        char one[] = {static_cast<char>(a)};
        auto decoded_one = decode(encode({one, 1}));
        ASSERT_TRUE(decoded_one.has_value()) << "a=" << a;
        EXPECT_EQ(*decoded_one, std::string_view(one, 1)) << "a=" << a;

        for (size_t b = 0; b < 256; ++b)
        {
            char two[] = {static_cast<char>(a), static_cast<char>(b)};
            auto decoded_two = decode(encode({two, 2}));
            ASSERT_TRUE(decoded_two.has_value()) << "a=" << a << " b=" << b;
            EXPECT_EQ(*decoded_two, std::string_view(two, 2)) << "a=" << a << " b=" << b;
        }
    }
}

/// Decode must reject characters outside the 0-9A-Za-z alphabet.
TEST(Base62, DecodeInvalid)
{
    for (std::string_view bad : {
             " "sv,
             "!"sv,
             "-1"sv,
             "1e "sv,
             "6o."sv,
             "Hold my beer"sv,
             "\x80"sv,
         })
    {
        EXPECT_FALSE(decode(bad).has_value()) << bad;
    }
}

} // namespace DB
