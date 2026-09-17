#include <Common/isValidUTF8.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringOrArrayToT.h>

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

    static void vector(const ColumnString::Chars & data, const ColumnString::Offsets & offsets, PaddedPODArray<UInt8> & res, size_t input_rows_count)
    {
        size_t prev_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
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
        for (size_t i = 0; i < input_rows_count; ++i)
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
