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
            const size_t value_length = offsets[row] - prev_offset;

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

            res_data.insert(ascii.data(), ascii.data() + ascii.size());
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
            const size_t ascii_length = offsets[row] - prev_offset;
            std::string_view ascii_view(ascii, ascii_length);

            unicode = ada::idna::to_unicode(ascii_view);

            res_data.insert(unicode.data(), unicode.data() + unicode.size());
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
    FunctionDocumentation::Description description_encode = R"(
Returns the ASCII representation (ToASCII algorithm) of a domain name according to the [Internationalized Domain Names in Applications](https://en.wikipedia.org/wiki/Internationalized_domain_name#Internationalizing_Domain_Names_in_Applications) (IDNA) mechanism.
The input string must be UTF-encoded and translatable to an ASCII string, otherwise an exception is thrown.

:::note
No percent decoding or trimming of tabs, spaces or control characters is performed.
:::
)";
    FunctionDocumentation::Syntax syntax_encode = "idnaEncode(s)";
    FunctionDocumentation::Arguments arguments_encode = {
        {"s", "Input string.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_encode = {"Returns an ASCII representation of the input string according to the IDNA mechanism of the input value.", {"String"}};
    FunctionDocumentation::Examples examples_encode = {
    {
        "Usage example",
        "SELECT idnaEncode('straße.münchen.de')",
        R"(
┌─idnaEncode('straße.münchen.de')─────┐
│ xn--strae-oqa.xn--mnchen-3ya.de     │
└─────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {24, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation_encode = {description_encode, syntax_encode, arguments_encode, returned_value_encode, examples_encode, introduced_in, category};

    FunctionDocumentation::Description description_try_encode = R"(
Returns the Unicode (UTF-8) representation (ToUnicode algorithm) of a domain name according to the [Internationalized Domain Names in Applications](https://en.wikipedia.org/wiki/Internationalized_domain_name#Internationalizing_Domain_Names_in_Applications) (IDNA) mechanism.
In case of an error it returns an empty string instead of throwing an exception.
)";
    FunctionDocumentation::Syntax syntax_try_encode = "tryIdnaEncode(s)";
    FunctionDocumentation::Arguments arguments_try_encode = {
        {"s", "Input string.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_try_encode = {"Returns an ASCII representation of the input string according to the IDNA mechanism of the input value, or empty string if input is invalid.", {"String"}};
    FunctionDocumentation::Examples examples_try_encode = {
    {
        "Usage example",
        "SELECT tryIdnaEncode('straße.münchen.de')",
        R"(
┌─tryIdnaEncode('straße.münchen.de')──┐
│ xn--strae-oqa.xn--mnchen-3ya.de     │
└─────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation documentation_try_encode = {description_try_encode, syntax_try_encode, arguments_try_encode, returned_value_try_encode, examples_try_encode, introduced_in, category};

    FunctionDocumentation::Description description_decode = R"(
Returns the Unicode (UTF-8) representation (ToUnicode algorithm) of a domain name according to the [Internationalized Domain Names in Applications](https://en.wikipedia.org/wiki/Internationalized_domain_name#Internationalizing_Domain_Names_in_Applications) (IDNA) mechanism.
In case of an error (e.g. because the input is invalid), the input string is returned.
Note that repeated application of [`idnaEncode()`](#idnaEncode) and [`idnaDecode()`](#idnaDecode) does not necessarily return the original string due to case normalization.
)";
    FunctionDocumentation::Syntax syntax_decode = "idnaDecode(s)";
    FunctionDocumentation::Arguments arguments_decode = {
        {"s", "Input string.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_decode = {"Returns a Unicode (UTF-8) representation of the input string according to the IDNA mechanism of the input value.", {"String"}};
    FunctionDocumentation::Examples examples_decode = {
    {
        "Usage example",
        "SELECT idnaDecode('xn--strae-oqa.xn--mnchen-3ya.de')",
        R"(
┌─idnaDecode('xn--strae-oqa.xn--mnchen-3ya.de')─┐
│ straße.münchen.de                             │
└───────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation documentation_decode = {description_decode, syntax_decode, arguments_decode, returned_value_decode, examples_decode, introduced_in, category};

    factory.registerFunction<FunctionIdnaEncode>(documentation_encode);
    factory.registerFunction<FunctionTryIdnaEncode>(documentation_try_encode);
    factory.registerFunction<FunctionIdnaDecode>(documentation_decode);
}

}

#endif
