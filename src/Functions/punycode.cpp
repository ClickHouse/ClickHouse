#include "config.h"

#if USE_IDNA

#include <Columns/ColumnString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wnewline-eof"
#include <ada/idna/punycode.h>
#include <ada/idna/unicode_transcoding.h>
#pragma clang diagnostic pop

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
}

/// Implementation of
/// - punycodeEncode(), punycodeDecode() and tryPunycodeDecode(), see https://en.wikipedia.org/wiki/Punycode

enum class ErrorHandling : uint8_t
{
    Throw,  /// Throw exception
    Empty   /// Return empty string
};


struct PunycodeEncode
{
    /// Encoding-as-punycode can only fail if the input isn't valid UTF8. In that case, return undefined output, i.e. garbage-in, garbage-out.
    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        res_data.reserve(data.size()); /// just a guess, assuming the input is all-ASCII
        res_offsets.reserve(input_rows_count);

        size_t prev_offset = 0;
        std::u32string value_utf32;
        std::string value_puny;
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const char * value = reinterpret_cast<const char *>(&data[prev_offset]);
            const size_t value_length = offsets[row] - prev_offset - 1;

            const size_t value_utf32_length = ada::idna::utf32_length_from_utf8(value, value_length);
            value_utf32.resize(value_utf32_length);
            const size_t codepoints = ada::idna::utf8_to_utf32(value, value_length, value_utf32.data());
            if (codepoints == 0)
                value_utf32.clear(); /// input was empty or no valid UTF-8

            const bool ok = ada::idna::utf32_to_punycode(value_utf32, value_puny);
            if (!ok)
                value_puny.clear();

            res_data.insert(value_puny.c_str(), value_puny.c_str() + value_puny.size() + 1);
            res_offsets.push_back(res_data.size());

            prev_offset = offsets[row];

            value_utf32.clear();
            value_puny.clear(); /// utf32_to_punycode() appends to its output string
        }
    }

    [[noreturn]] static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &, size_t)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Arguments of type FixedString are not allowed");
    }
};


template <ErrorHandling error_handling>
struct PunycodeDecode
{
    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        res_data.reserve(data.size()); /// just a guess, assuming the input is all-ASCII
        res_offsets.reserve(input_rows_count);

        size_t prev_offset = 0;
        std::u32string value_utf32;
        std::string value_utf8;
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const char * value = reinterpret_cast<const char *>(&data[prev_offset]);
            const size_t value_length = offsets[row] - prev_offset - 1;

            const std::string_view value_punycode(value, value_length);
            const bool ok = ada::idna::punycode_to_utf32(value_punycode, value_utf32);
            if (!ok)
            {
                if constexpr (error_handling == ErrorHandling::Throw)
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "'{}' is not a valid Punycode-encoded string", value_punycode);
                }
                else
                {
                    static_assert(error_handling == ErrorHandling::Empty);
                    value_utf32.clear();
                }
            }

            const size_t utf8_length = ada::idna::utf8_length_from_utf32(value_utf32.data(), value_utf32.size());
            value_utf8.resize(utf8_length);
            ada::idna::utf32_to_utf8(value_utf32.data(), value_utf32.size(), value_utf8.data());

            res_data.insert(value_utf8.c_str(), value_utf8.c_str() + value_utf8.size() + 1);
            res_offsets.push_back(res_data.size());

            prev_offset = offsets[row];

            value_utf32.clear(); /// punycode_to_utf32() appends to its output string
            value_utf8.clear();
        }
    }

    [[noreturn]] static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &, size_t)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Arguments of type FixedString are not allowed");
    }
};

struct NamePunycodeEncode { static constexpr auto name = "punycodeEncode"; };
struct NamePunycodeDecode { static constexpr auto name = "punycodeDecode"; };
struct NameTryPunycodeDecode { static constexpr auto name = "tryPunycodeDecode"; };

using FunctionPunycodeEncode = FunctionStringToString<PunycodeEncode, NamePunycodeEncode>;
using FunctionPunycodeDecode = FunctionStringToString<PunycodeDecode<ErrorHandling::Throw>, NamePunycodeDecode>;
using FunctionTryPunycodeDecode = FunctionStringToString<PunycodeDecode<ErrorHandling::Empty>, NameTryPunycodeDecode>;

REGISTER_FUNCTION(Punycode)
{
    factory.registerFunction<FunctionPunycodeEncode>(FunctionDocumentation{
        .description=R"(
Computes a Punycode representation of a string.)",
        .syntax="punycodeEncode(str)",
        .arguments={{"str", "Input string"}},
        .returned_value="The punycode representation [String](/docs/en/sql-reference/data-types/string.md).",
        .examples={
            {"simple",
            "SELECT punycodeEncode('München') AS puny;",
            R"(
┌─puny───────┐
│ Mnchen-3ya │
└────────────┘
            )"
            }}
    });

    factory.registerFunction<FunctionPunycodeDecode>(FunctionDocumentation{
        .description=R"(
Computes a Punycode representation of a string. Throws an exception if the input is not valid Punycode.)",
        .syntax="punycodeDecode(str)",
        .arguments={{"str", "A Punycode-encoded string"}},
        .returned_value="The plaintext representation [String](/docs/en/sql-reference/data-types/string.md).",
        .examples={
            {"simple",
            "SELECT punycodeDecode('Mnchen-3ya') AS plain;",
            R"(
┌─plain───┐
│ München │
└─────────┘
            )"
            }}
    });

    factory.registerFunction<FunctionTryPunycodeDecode>(FunctionDocumentation{
        .description=R"(
Computes a Punycode representation of a string. Returns an empty string if the input is not valid Punycode.)",
        .syntax="punycodeDecode(str)",
        .arguments={{"str", "A Punycode-encoded string"}},
        .returned_value="The plaintext representation [String](/docs/en/sql-reference/data-types/string.md).",
        .examples={
            {"simple",
            "SELECT tryPunycodeDecode('Mnchen-3ya') AS plain;",
            R"(
┌─plain───┐
│ München │
└─────────┘
            )"
            }}
    });
}

}

#endif
