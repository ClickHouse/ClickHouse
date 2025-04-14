#include "config.h"

#if USE_IDNA

#include <Columns/ColumnString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wnewline-eof"
#include <ada/idna/to_ascii.h>
#include <ada/idna/to_unicode.h>
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
/// - idnaEncode(), tryIdnaEncode() and idnaDecode(), see https://en.wikipedia.org/wiki/Internationalized_domain_name#ToASCII_and_ToUnicode
///   and [3] https://www.unicode.org/reports/tr46/#ToUnicode

enum class ErrorHandling : uint8_t
{
    Throw,  /// Throw exception
    Empty   /// Return empty string
};


/// Translates a UTF-8 string (typically an Internationalized Domain Name for Applications, IDNA) to an ASCII-encoded equivalent. The
/// encoding is performed per domain component and based on Punycode with ASCII Compatible Encoding (ACE) prefix "xn--".
/// Example: "straße.münchen.de" --> "xn--strae-oqa.xn--mnchen-3ya.de"
/// Note: doesn't do percent decoding. Doesn't trim tabs, spaces or control characters. Expects non-empty inputs.
template <ErrorHandling error_handling>
struct IdnaEncode
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
        std::string ascii;
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const char * value = reinterpret_cast<const char *>(&data[prev_offset]);
            const size_t value_length = offsets[row] - prev_offset - 1;

            std::string_view value_view(value, value_length);
            if (!value_view.empty()) /// to_ascii() expects non-empty input
            {
                ascii = ada::idna::to_ascii(value_view);
                const bool ok = !ascii.empty();
                if (!ok)
                {
                    if constexpr (error_handling == ErrorHandling::Throw)
                    {
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'{}' cannot be converted to ASCII", value_view);
                    }
                    else
                    {
                        static_assert(error_handling == ErrorHandling::Empty);
                        ascii.clear();
                    }
                }
            }

            res_data.insert(ascii.c_str(), ascii.c_str() + ascii.size() + 1);
            res_offsets.push_back(res_data.size());

            prev_offset = offsets[row];

            ascii.clear();
        }
    }

    [[noreturn]] static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &, size_t)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Arguments of type FixedString are not allowed");
    }
};

/// Translates an ASII-encoded IDNA string back to its UTF-8 representation.
struct IdnaDecode
{
    /// As per the specification, invalid inputs are returned as is, i.e. there is no special error handling.
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
        std::string unicode;
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const char * ascii = reinterpret_cast<const char *>(&data[prev_offset]);
            const size_t ascii_length = offsets[row] - prev_offset - 1;
            std::string_view ascii_view(ascii, ascii_length);

            unicode = ada::idna::to_unicode(ascii_view);

            res_data.insert(unicode.c_str(), unicode.c_str() + unicode.size() + 1);
            res_offsets.push_back(res_data.size());

            prev_offset = offsets[row];

            unicode.clear();
        }
    }

    [[noreturn]] static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &, size_t)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Arguments of type FixedString are not allowed");
    }
};

struct NameIdnaEncode { static constexpr auto name = "idnaEncode"; };
struct NameTryIdnaEncode { static constexpr auto name = "tryIdnaEncode"; };
struct NameIdnaDecode { static constexpr auto name = "idnaDecode"; };

using FunctionIdnaEncode = FunctionStringToString<IdnaEncode<ErrorHandling::Throw>, NameIdnaEncode>;
using FunctionTryIdnaEncode = FunctionStringToString<IdnaEncode<ErrorHandling::Empty>, NameTryIdnaEncode>;
using FunctionIdnaDecode = FunctionStringToString<IdnaDecode, NameIdnaDecode>;

REGISTER_FUNCTION(Idna)
{
    factory.registerFunction<FunctionIdnaEncode>(FunctionDocumentation{
        .description=R"(
Computes an ASCII representation of an Internationalized Domain Name. Throws an exception in case of error.)",
        .syntax="idnaEncode(str)",
        .arguments={{"str", "Input string"}},
        .returned_value="An ASCII-encoded domain name [String](/docs/en/sql-reference/data-types/string.md).",
        .examples={
            {"simple",
            "SELECT idnaEncode('straße.münchen.de') AS ascii;",
            R"(
┌─ascii───────────────────────────┐
│ xn--strae-oqa.xn--mnchen-3ya.de │
└─────────────────────────────────┘
            )"
            }}
    });

    factory.registerFunction<FunctionTryIdnaEncode>(FunctionDocumentation{
        .description=R"(
Computes a ASCII representation of an Internationalized Domain Name. Returns an empty string in case of error)",
        .syntax="punycodeEncode(str)",
        .arguments={{"str", "Input string"}},
        .returned_value="An ASCII-encoded domain name [String](/docs/en/sql-reference/data-types/string.md).",
        .examples={
            {"simple",
            "SELECT idnaEncodeOrNull('München') AS ascii;",
            R"(
┌─ascii───────────────────────────┐
│ xn--strae-oqa.xn--mnchen-3ya.de │
└─────────────────────────────────┘
            )"
            }}
    });

    factory.registerFunction<FunctionIdnaDecode>(FunctionDocumentation{
        .description=R"(
Computes the Unicode representation of ASCII-encoded Internationalized Domain Name.)",
        .syntax="idnaDecode(str)",
        .arguments={{"str", "Input string"}},
        .returned_value="An Unicode-encoded domain name [String](/docs/en/sql-reference/data-types/string.md).",
        .examples={
            {"simple",
            "SELECT idnaDecode('xn--strae-oqa.xn--mnchen-3ya.de') AS unicode;",
            R"(
┌─unicode───────────┐
│ straße.münchen.de │
└───────────────────┘
            )"
            }}
    });
}

}

#endif
