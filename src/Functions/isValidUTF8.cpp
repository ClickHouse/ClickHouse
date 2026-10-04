#include <Common/isValidUTF8.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringOrArrayToT.h>

#include <algorithm>

#include "config.h"

#if USE_SIMDUTF
#    include <simdutf.h>
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

struct ValidUTF8Impl
{
    static UInt8 isValidUTF8(const UInt8 * data, UInt64 len) { return DB::UTF8::isValidUTF8(data, len); }

    static constexpr bool is_fixed_to_constant = false;

    static size_t asciiPrefixLength([[maybe_unused]] const UInt8 * data, [[maybe_unused]] size_t len)
    {
#if USE_SIMDUTF
        return simdutf::validate_ascii_with_errors(reinterpret_cast<const char *>(data), len).count;
#else
        return 0;
#endif
    }

    static void vector(const ColumnString::Chars & data, const ColumnString::Offsets & offsets, PaddedPODArray<UInt8> & res, size_t input_rows_count)
    {
        if (input_rows_count == 0)
            return;

        /// A row that ends inside the column's leading run of ASCII bytes is valid UTF-8.
        const size_t prefix = asciiPrefixLength(data.data(), offsets[input_rows_count - 1]);
        const size_t ascii_rows = std::upper_bound(offsets.begin(), offsets.begin() + input_rows_count, prefix) - offsets.begin();
        std::fill(res.begin(), res.begin() + ascii_rows, 1);
        if (ascii_rows == input_rows_count)
            return;

        res[ascii_rows] = isValidUTF8(data.data() + prefix, offsets[ascii_rows] - prefix);
        size_t prev_offset = offsets[ascii_rows];
        for (size_t i = ascii_rows + 1; i < input_rows_count; ++i)
        {
            res[i] = isValidUTF8(data.data() + prev_offset, offsets[i] - prev_offset);
            prev_offset = offsets[i];
        }
    }

    static void vectorFixedToConstant(const ColumnString::Chars &, size_t, UInt8 &, size_t)
    {
    }

    static void vectorFixedToVector(const ColumnString::Chars & data, size_t n, PaddedPODArray<UInt8> & res, size_t input_rows_count)
    {
        const size_t prefix = asciiPrefixLength(data.data(), n * input_rows_count);
        const size_t ascii_rows = prefix / n;
        std::fill(res.begin(), res.begin() + ascii_rows, 1);
        if (ascii_rows == input_rows_count)
            return;

        res[ascii_rows] = isValidUTF8(data.data() + prefix, (ascii_rows + 1) * n - prefix);
        for (size_t i = ascii_rows + 1; i < input_rows_count; ++i)
            res[i] = isValidUTF8(data.data() + i * n, n);
    }

    [[noreturn]] static void array(const ColumnString::Offsets &, PaddedPODArray<UInt8> &, size_t)
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cannot apply function isValidUTF8 to Array argument");
    }

    [[noreturn]] static void uuid(const ColumnUUID::Container &, size_t &, PaddedPODArray<UInt8> &, size_t)
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cannot apply function isValidUTF8 to UUID argument");
    }

    [[noreturn]] static void ipv6(const ColumnIPv6::Container &, size_t &, PaddedPODArray<UInt8> &, size_t)
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cannot apply function isValidUTF8 to IPv6 argument");
    }

    [[noreturn]] static void ipv4(const ColumnIPv4::Container &, size_t &, PaddedPODArray<UInt8> &, size_t)
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cannot apply function isValidUTF8 to IPv4 argument");
    }
};

struct NameIsValidUTF8
{
    static constexpr auto name = "isValidUTF8";
};
using FunctionValidUTF8 = FunctionStringOrArrayToT<ValidUTF8Impl, NameIsValidUTF8, UInt8>;

REGISTER_FUNCTION(IsValidUTF8)
{
    FunctionDocumentation::Description description = R"(
Checks if the set of bytes constitutes valid UTF-8-encoded text.
)";
    FunctionDocumentation::Syntax syntax = "isValidUTF8(s)";
    FunctionDocumentation::Arguments arguments = {
        {"s", "The string to check for UTF-8 encoded validity.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1`, if the set of bytes constitutes valid UTF-8-encoded text, otherwise `0`.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(SELECT isValidUTF8('\\xc3\\xb1') AS valid, isValidUTF8('\\xc3\\x28') AS invalid)",
        R"(
┌─valid─┬─invalid─┐
│     1 │       1 │
└───────┴─────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionValidUTF8>(documentation);
}

}
