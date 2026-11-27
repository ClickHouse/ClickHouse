#include <Common/isValidUTF8.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringOrArrayToT.h>

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

    /// Row i is the bytes [row_begin(i), row_begin(i + 1)).
    template <typename RowBegin>
    static void validateRows(const UInt8 * data, size_t rows, RowBegin row_begin, PaddedPODArray<UInt8> & res)
    {
#if USE_SIMDUTF
        const size_t data_end = row_begin(rows);
        size_t row = 0;
        while (row < rows)
        {
            /// The bytes before valid_end are valid UTF-8, so a row there is valid iff it does not end in the middle of a
            /// code point, i.e. the byte after it is not a continuation byte. Its start is checked as the previous row's end.
            const size_t begin = row_begin(row);
            const size_t valid_end
                = begin + simdutf::validate_utf8_with_errors(reinterpret_cast<const char *>(data + begin), data_end - begin).count;
            for (; row < rows; ++row)
            {
                const size_t end = row_begin(row + 1);
                if (end >= valid_end || (data[end] & 0xC0) == 0x80)
                    break;
                res[row] = 1;
            }
            for (; row < rows && row_begin(row + 1) == valid_end; ++row)
                res[row] = 1;
            /// This row contains the first invalid byte or ends in the middle of a code point.
            if (row < rows)
                res[row++] = 0;
        }
#else
        for (size_t row = 0; row < rows; ++row)
            res[row] = isValidUTF8(data + row_begin(row), row_begin(row + 1) - row_begin(row));
#endif
    }

    static void vector(const ColumnString::Chars & data, const ColumnString::Offsets & offsets, PaddedPODArray<UInt8> & res, size_t input_rows_count)
    {
        validateRows(data.data(), input_rows_count, [&](size_t i) { return offsets[static_cast<ssize_t>(i) - 1]; }, res);
    }

    static void vectorFixedToConstant(const ColumnString::Chars &, size_t, UInt8 &, size_t)
    {
    }

    static void vectorFixedToVector(const ColumnString::Chars & data, size_t n, PaddedPODArray<UInt8> & res, size_t input_rows_count)
    {
        validateRows(data.data(), input_rows_count, [n](size_t i) { return i * n; }, res);
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
