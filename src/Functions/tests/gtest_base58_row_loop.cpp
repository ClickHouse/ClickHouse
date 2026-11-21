#include <gtest/gtest.h>

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Functions/FunctionBase58Conversion.h>

#include <cstddef>
#include <string>
#include <string_view>

using namespace DB;

namespace
{

/// The function names live in anonymous namespaces in base58Encode.cpp and base58Decode.cpp, so the row
/// loops are instantiated here with local equivalents.
struct NameBase58Encode
{
    static constexpr auto name = "base58Encode";
};

struct NameBase58Decode
{
    static constexpr auto name = "base58Decode";
};

using Base58EncodeRowLoop = BaseXXEncode<Base58EncodeTraits, NameBase58Encode>;
using Base58DecodeRowLoop = BaseXXDecode<Base58DecodeTraits, NameBase58Decode, BaseXXDecodeErrorHandling::ThrowException>;

/// The same body gtest_base58.cpp converts, so the counts below are comparable with the ones asserted
/// there. A non-zero first byte keeps the whole value on the generic path.
std::string bodyOfLength(size_t length)
{
    std::string body(length, '\0');
    for (size_t i = 0; i < length; ++i)
        body[i] = static_cast<char>(i == 0 ? 1 : (i * 137 + 29) % 256);
    return body;
}

}

/// The count spans the block's rows, so values individually too small to reach a check of their own still
/// add up to one. The counts are integer functions of the exact input, so they are asserted exactly.
TEST(Base58RowLoop, CancellationCountAcrossRows)
{
    /// 1025 bytes converts in about 0.68 of the work between two checks, so eight rows are worth five.
    constexpr size_t rows = 8;
    constexpr size_t length = 1025;
    constexpr size_t expected_encode_calls = 5;
    constexpr size_t expected_decode_calls = 5;

    const std::string body = bodyOfLength(length);

    auto src_fixed = ColumnFixedString::create(length);
    for (size_t row = 0; row < rows; ++row)
        src_fixed->insertData(body.data(), body.size());

    ColumnString::MutablePtr encoded = ColumnString::create();
    size_t encode_work = 0;
    size_t encode_calls = 0;
    Base58EncodeRowLoop::processFixedString<false>(
        *src_fixed, encoded, rows, 0, Base58EncodeTraits::max_input_size, [&] { ++encode_calls; }, encode_work);
    ASSERT_EQ(encoded->size(), rows);
    EXPECT_EQ(encode_calls, expected_encode_calls);

    const std::string encoded_text(encoded->getDataAt(0));

    auto src_string = ColumnString::create();
    for (size_t row = 0; row < rows; ++row)
        src_string->insertData(encoded_text.data(), encoded_text.size());

    ColumnString::MutablePtr decoded = ColumnString::create();
    size_t decode_work = 0;
    size_t decode_calls = 0;
    Base58DecodeRowLoop::processString<false>(
        *src_string, decoded, rows, 0, Base58DecodeTraits::max_input_size, [&] { ++decode_calls; }, decode_work);
    ASSERT_EQ(decoded->size(), rows);
    ASSERT_EQ(decoded->getDataAt(rows - 1), std::string_view(body));
    EXPECT_EQ(decode_calls, expected_decode_calls);
}
