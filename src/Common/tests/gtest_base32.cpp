#include <Common/Base32.h>

#include <string_view>

#include <gtest/gtest.h>

using namespace std::string_view_literals;

namespace DB
{

static std::string to_hex(const UInt8 * data, size_t length)
{
    static constexpr char hex_chars[] = "0123456789ABCDEF";
    std::string result;
    result.reserve(length * 2);

    for (size_t i = 0; i < length; ++i)
    {
        result += hex_chars[(data[i] >> 4) & 0x0F];
        result += hex_chars[data[i] & 0x0F];
    }

    return result;
}

struct EncodeDecodeData
{
    int id;
    std::string_view input;
    std::string_view expected;
};

struct InvalidData
{
    int id;
    std::string_view input;
};

template <typename TestData, typename Coder>
struct CoderTestConfiguration
{
    using test_data_t = TestData;
    using coder_t = Coder;
};

struct Base32Rfc4648TestData
{
    constexpr static bool allow_lowercase_encoded = true;

    // clang-format off
    constexpr static EncodeDecodeData encode_tests[] = {
        {100, ""sv, ""sv},
        {101, "f"sv, "MY======"sv},
        {102, "fo"sv, "MZXQ===="sv},
        {103, "foo"sv, "MZXW6==="sv},
        {104, "foob"sv, "MZXW6YQ="sv},
        {105, "fooba"sv, "MZXW6YTB"sv},
        {106, "foobar"sv, "MZXW6YTBOI======"sv},
        
        {200, "\x00"sv, "AA======"sv},
        {201, "\x00\x00"sv, "AAAA===="sv},
        {202, "\x00\x00\x00"sv, "AAAAA==="sv},
        {203, "\x00\x00\x00\x00"sv, "AAAAAAA="sv},
        {204, "\x00\x00\x00\x00\x00"sv, "AAAAAAAA"sv},

        {300, "\xFF"sv, "74======"sv},
        {301, "\xFF\xFF"sv, "777Q===="sv},
        {302, "\xFF\xFF\xFF"sv, "77776==="sv},
        {303, "\xFF\xFF\xFF\xFF"sv, "777777Y="sv},
        {304, "\xFF\xFF\xFF\xFF\xFF"sv, "77777777"sv},
        
        {400, "\x01\x23\x45\x67\x89"sv, "AERUKZ4J"sv},
        {401, "\xAB\xCD\xEF\x01\x23"sv, "VPG66AJD"sv},
        
        {400, "1234567890"sv, "GEZDGNBVGY3TQOJQ"sv},
        {401, "The quick brown fox jumps over the lazy dog"sv, 
        "KRUGKIDROVUWG2ZAMJZG653OEBTG66BANJ2W24DTEBXXMZLSEB2GQZJANRQXU6JAMRXWO==="sv},
        
        {500, "a"sv, "ME======"sv},          // 1 byte → 8 chars (2+6 padding)
        {501, "ab"sv, "MFRA===="sv},         // 2 bytes → 8 chars (4+4 padding)
        {502, "abc"sv, "MFRGG==="sv},        // 3 bytes → 8 chars (5+3 padding)
        {503, "abcd"sv, "MFRGGZA="sv},       // 4 bytes → 8 chars (7+1 padding)
        {504, "abcde"sv, "MFRGGZDF"sv},      // 5 bytes → 8 chars (no padding)
        {505, "abcdef"sv, "MFRGGZDFMY======"sv}, // 6 bytes → 16 chars (4+12 padding)
    };
    constexpr static InvalidData bad_decode_tests[] = {
        {100, "========"sv}, // Invalid padding
        {101, "MZXW6YT!"sv}, // Invalid character
        {102, "MZXW6Y=B"sv}, // Padding in wrong place
        {103, "MZXW6Y=!"sv}, // Invalid character and padding
        {104, "MZXW6Y==="sv}, // Invalid padding length
        {105, "MZXW6YQ=Q"sv}, // Extra character after padding
        {106, "MZXW6YQ======"sv}, // Too much padding
        {107, "12345678"sv}, // Characters not in Base32 alphabet
        {108, "MZXW6YQ"sv}, // Missing padding
        {109, "MZXW6YQ=="sv}, // Incorrect padding length
        {110, "MZXW6YQ==="sv}, // Excessive padding
        {111, "MZXW6YQ===="sv}, // Invalid padding sequence
        {112, "MZXW6YQ====="sv}, // Too much padding
        {113, "MZXW6YQ======"sv}, // Excessive padding
        {114, "MZXW6YQ======="sv}, // Invalid padding length
        {115, "MZXW6YQ====!=="sv}, // Invalid character in padding
        {116, "MZXW6YQ====A=="sv}, // Extra character in padding
        {117, "MZXW6YQ======"sv}, // Invalid padding with valid length
        {118, "MZXW6Y=="sv}, // Invalid padding length
    };
    // clang-format on
};

template <typename T>
struct BaseCoderTest : public ::testing::Test
{
    using test_data_t = typename T::test_data_t;
    using coder_t = typename T::coder_t;
};

using TestedTypes = ::testing::Types<CoderTestConfiguration<Base32Rfc4648TestData, Base32<Base32Rfc4648, Base32NaiveTag>>>;
TYPED_TEST_SUITE(BaseCoderTest, TestedTypes);

TYPED_TEST(BaseCoderTest, Null)
{
    using coder_t = typename TypeParam::coder_t;
    UInt8 output[64] = {0};
    {
        size_t const result = coder_t::encodeBase32(nullptr, 0, output);
        EXPECT_EQ(result, 0) << " for empty input encode";
    }
    {
        auto const result = coder_t::decodeBase32(nullptr, 0, output);
        ASSERT_TRUE(result.has_value()) << " for empty input decode";
        EXPECT_EQ(result.value(), 0) << " for empty input decode";
    }
}

TYPED_TEST(BaseCoderTest, EncodeDecode)
{
    using test_data_t = typename TypeParam::test_data_t;
    using coder_t = typename TypeParam::coder_t;
    for (const auto & test : test_data_t::encode_tests)
    {
        size_t const input_len = test.input.size();
        size_t const expected_output_len = ((input_len + 4) / 5) * 8;
        UInt8 encode_output[128] = {0};

        size_t const encoded_result_len
            = coder_t::encodeBase32(reinterpret_cast<const UInt8 *>(test.input.data()), input_len, encode_output);

        ASSERT_EQ(expected_output_len, encoded_result_len) << " for id=" << test.id;
        auto const encode_output_sv = std::string_view(reinterpret_cast<const char *>(encode_output), encoded_result_len);
        EXPECT_EQ(test.expected, encode_output_sv) << " for id=" << test.id;

        auto decode = [&](std::string_view what)
        {
            UInt8 decode_output[128] = {0};
            auto const decode_result = coder_t::decodeBase32(reinterpret_cast<const UInt8 *>(what.data()), what.size(), decode_output);
            ASSERT_TRUE(decode_result.has_value()) << " for id=" << test.id;
            EXPECT_EQ(input_len, decode_result.value()) << " for id=" << test.id;
            auto const decode_output_sv = std::string_view(reinterpret_cast<const char *>(decode_output), decode_result.value());
            EXPECT_EQ(test.input, decode_output_sv) << " for id=" << test.id;
        };
        // And now decode it back
        decode(encode_output_sv);
        if constexpr (test_data_t::allow_lowercase_encoded)
        {
            std::string lower_case(encode_output_sv);
            std::transform(lower_case.begin(), lower_case.end(), lower_case.begin(), ::tolower);
            decode(lower_case);
        }
    }
}

TYPED_TEST(BaseCoderTest, DecodeInvalid)
{
    using test_data_t = typename TypeParam::test_data_t;
    using coder_t = typename TypeParam::coder_t;
    for (const auto & test : test_data_t::bad_decode_tests)
    {
        size_t const input_len = test.input.size();
        UInt8 output[128] = {0};

        auto const result = coder_t::decodeBase32(reinterpret_cast<const UInt8 *>(test.input.data()), input_len, output);

        ASSERT_FALSE(result.has_value()) << " for id=" << test.id;
    }
}

TYPED_TEST(BaseCoderTest, LargeTest)
{
    using coder_t = typename TypeParam::coder_t;
    size_t const large_size = 1234;
    std::vector<UInt8> large_input(large_size);
    for (size_t i = 0; i < large_size; ++i)
    {
        large_input[i] = static_cast<UInt8>(i % 256);
    }

    size_t const encoded_len = ((large_size + 4) / 5) * 8;
    std::vector<UInt8> encoded(encoded_len + 1);
    auto const actual_encoded_len = coder_t::encodeBase32(large_input.data(), large_size, encoded.data());
    ASSERT_EQ(encoded_len, actual_encoded_len) << " for input (as hex) " << to_hex(large_input.data(), large_size);

    std::vector<UInt8> decoded(large_size);
    auto const decode_result = coder_t::decodeBase32(encoded.data(), encoded_len, decoded.data());

    ASSERT_TRUE(decode_result.has_value()) << " for input (as hex) " << to_hex(large_input.data(), large_size);
    ASSERT_EQ(large_size, decode_result.value()) << " for input (as hex) " << to_hex(large_input.data(), large_size);
    EXPECT_EQ(0, memcmp(large_input.data(), decoded.data(), large_size))
        << " for input (as hex) " << to_hex(large_input.data(), large_size);
}

}
