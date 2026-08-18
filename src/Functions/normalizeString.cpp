#include "config.h"

#if USE_ICU
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <unicode/rep.h>
#include <unicode/unistr.h>
#include <unicode/unorm2.h>
#include <unicode/ustring.h>
#include <unicode/utypes.h>
#include <Common/logger_useful.h>
#include <Columns/ColumnString.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int CANNOT_NORMALIZE_STRING;
}

namespace
{

// Expansion factors are specified for UTF-32, since icu uses UTF-32 for normalization
// Maximum expansion factors for different normalization forms
// https://unicode.org/faq/normalization.html#12

struct NormalizeNFCImpl
{
    static constexpr auto name = "normalizeUTF8NFC";

    static constexpr auto expansionFactor = 3;

    static const UNormalizer2 *getNormalizer(UErrorCode *err)
    {
        return unorm2_getNFCInstance(err);
    }
};

struct NormalizeNFDImpl
{
    static constexpr auto name = "normalizeUTF8NFD";

    static constexpr auto expansionFactor = 4;

    static const UNormalizer2 *getNormalizer(UErrorCode *err)
    {
        return unorm2_getNFDInstance(err);
    }
};

struct NormalizeNFKCImpl
{
    static constexpr auto name = "normalizeUTF8NFKC";

    static constexpr auto expansionFactor = 18;

    static const UNormalizer2 *getNormalizer(UErrorCode *err)
    {
        return unorm2_getNFKCInstance(err);
    }
};


struct NormalizeNFKDImpl
{
    static constexpr auto name = "normalizeUTF8NFKD";

    static constexpr auto expansionFactor = 18;

    static const UNormalizer2 *getNormalizer(UErrorCode *err)
    {
        return unorm2_getNFKDInstance(err);
    }
};

template<typename NormalizeImpl>
struct NormalizeUTF8Impl
{
    static void vector(const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        UErrorCode err = U_ZERO_ERROR;

        const UNormalizer2 *normalizer = NormalizeImpl::getNormalizer(&err);
        if (U_FAILURE(err))
            throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Normalization failed (getNormalizer): {}", u_errorName(err));

        res_offsets.resize(input_rows_count);
        res_data.reserve(data.size() * 2);

        ColumnString::Offset current_from_offset = 0;
        ColumnString::Offset current_to_offset = 0;

        PODArray<UChar> from_uchars;
        PODArray<UChar> to_uchars;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            size_t from_size = offsets[i] - current_from_offset;

            if (from_size > 0)
            {
                from_uchars.resize(from_size);
                int32_t from_code_points = 0;
                u_strFromUTF8(
                    from_uchars.data(),
                    from_uchars.size(),
                    &from_code_points,
                    reinterpret_cast<const char*>(&data[current_from_offset]),
                    from_size,
                    &err);
                if (U_FAILURE(err))
                    throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Normalization failed (strFromUTF8): {}", u_errorName(err));

                to_uchars.resize(from_code_points * NormalizeImpl::expansionFactor);

                int32_t to_code_points = unorm2_normalize(
                    normalizer,
                    from_uchars.data(),
                    from_code_points,
                    to_uchars.data(),
                    to_uchars.size(),
                    &err);
                if (U_FAILURE(err))
                    throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Normalization failed (normalize): {}", u_errorName(err));

                size_t max_to_size = current_to_offset + 4 * to_code_points;
                if (res_data.size() < max_to_size)
                    res_data.resize(max_to_size);

                int32_t to_size = 0;
                u_strToUTF8(
                    reinterpret_cast<char*>(&res_data[current_to_offset]),
                    res_data.size() - current_to_offset,
                    &to_size,
                    to_uchars.data(),
                    to_code_points,
                    &err);
                if (U_FAILURE(err))
                    throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Normalization failed (strToUTF8): {}", u_errorName(err));

                current_to_offset += to_size;
            }

            res_offsets[i] = current_to_offset;
            current_from_offset = offsets[i];
        }

        res_data.resize(current_to_offset);
    }

    [[noreturn]] static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &, size_t)
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot apply function normalizeUTF8 to fixed string.");
    }
};

using FunctionNormalizeUTF8NFC = FunctionStringToString<NormalizeUTF8Impl<NormalizeNFCImpl>, NormalizeNFCImpl>;
using FunctionNormalizeUTF8NFD = FunctionStringToString<NormalizeUTF8Impl<NormalizeNFDImpl>, NormalizeNFDImpl>;
using FunctionNormalizeUTF8NFKC = FunctionStringToString<NormalizeUTF8Impl<NormalizeNFKCImpl>, NormalizeNFKCImpl>;
using FunctionNormalizeUTF8NFKD = FunctionStringToString<NormalizeUTF8Impl<NormalizeNFKDImpl>, NormalizeNFKDImpl>;
}

REGISTER_FUNCTION(NormalizeUTF8)
{
    FunctionDocumentation::Description description_nfc = R"(
Normalizes a UTF-8 string according to the [NFC normalization form](https://en.wikipedia.org/wiki/Unicode_equivalence#Normal_forms).
)";
    FunctionDocumentation::Syntax syntax_nfc = "normalizeUTF8NFC(str)";
    FunctionDocumentation::Arguments arguments_nfc = {
        {"str", "UTF-8 encoded input string.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_nfc = {"Returns the NFC normalized form of the UTF-8 string.", {"String"}};
    FunctionDocumentation::Examples examples_nfc = {
    {
        "Usage example",
        R"(
SELECT
'é' AS original, -- e + combining acute accent (U+0065 + U+0301)
length(original),
normalizeUTF8NFC('é') AS nfc_normalized, -- é (U+00E9)
length(nfc_normalized);
        )",
        R"(
┌─original─┬─length(original)─┬─nfc_normalized─┬─length(nfc_normalized)─┐
│ é        │                2 │ é              │                      2 │
└──────────┴──────────────────┴────────────────┴────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {21, 11};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation_nfc = {description_nfc, syntax_nfc, arguments_nfc, returned_value_nfc, examples_nfc, introduced_in, category};

    FunctionDocumentation::Description description_nfd = R"(
Normalizes a UTF-8 string according to the [NFD normalization form](https://en.wikipedia.org/wiki/Unicode_equivalence#Normal_forms).
)";
    FunctionDocumentation::Syntax syntax_nfd = "normalizeUTF8NFD(str)";
    FunctionDocumentation::Arguments arguments_nfd = {
        {"str", "UTF-8 encoded input string.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_nfd = {"Returns the NFD normalized form of the UTF-8 string.", {"String"}};
    FunctionDocumentation::Examples examples_nfd = {
    {
        "Usage example",
        R"(
SELECT
    'é' AS original, -- é (U+00E9)
    length(original),
    normalizeUTF8NFD('é') AS nfd_normalized, -- e + combining acute (U+0065 + U+0301)
    length(nfd_normalized);
        )",
        R"(
┌─original─┬─length(original)─┬─nfd_normalized─┬─length(nfd_normalized)─┐
│ é        │                2 │ é              │                      3 │
└──────────┴──────────────────┴────────────────┴────────────────────────┘
        )"
    }
    };
    FunctionDocumentation documentation_nfd = {description_nfd, syntax_nfd, arguments_nfd, returned_value_nfd, examples_nfd, introduced_in, category};

    FunctionDocumentation::Description description_nfkc = R"(
Normalizes a UTF-8 string according to the [NFKC normalization form](https://en.wikipedia.org/wiki/Unicode_equivalence#Normal_forms).
)";
    FunctionDocumentation::Syntax syntax_nfkc = "normalizeUTF8NFKC(str)";
    FunctionDocumentation::Arguments arguments_nfkc = {
        {"str", "UTF-8 encoded input string.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_nfkc = {"Returns the NFKC normalized form of the UTF-8 string.", {"String"}};
    FunctionDocumentation::Examples examples_nfkc = {
    {
        "Usage example",
        R"(
SELECT
    '① ② ③' AS original,                            -- Circled number characters
    normalizeUTF8NFKC('① ② ③') AS nfkc_normalized;  -- Converts to 1 2 3
    )",
        R"(
┌─original─┬─nfkc_normalized─┐
│ ① ② ③  │ 1 2 3           │
└──────────┴─────────────────┘
    )"
    }
    };
    FunctionDocumentation documentation_nfkc = {description_nfkc, syntax_nfkc, arguments_nfkc, returned_value_nfkc, examples_nfkc, introduced_in, category};

    FunctionDocumentation::Description description_nfkd = R"(
Normalizes a UTF-8 string according to the [NFKD normalization form](https://en.wikipedia.org/wiki/Unicode_equivalence#Normal_forms).
)";
    FunctionDocumentation::Syntax syntax_nfkd = "normalizeUTF8NFKD(str)";
    FunctionDocumentation::Arguments arguments_nfkd = {
        {"str", "UTF-8 encoded input string.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_nfkd = {"Returns the NFKD normalized form of the UTF-8 string.", {"String"}};
    FunctionDocumentation::Examples examples_nfkd = {
    {
        "Usage example",
        R"(
SELECT
    'H₂O²' AS original,                            -- H + subscript 2 + O + superscript 2
    normalizeUTF8NFKD('H₂O²') AS nfkd_normalized;  -- Converts to H 2 O 2
        )",
        R"(
┌─original─┬─nfkd_normalized─┐
│ H₂O²     │ H2O2            │
└──────────┴─────────────────┘
        )"
    }
    };
    FunctionDocumentation documentation_nfkd = {description_nfkd, syntax_nfkd, arguments_nfkd, returned_value_nfkd, examples_nfkd, introduced_in, category};

    factory.registerFunction<FunctionNormalizeUTF8NFC>(documentation_nfc);
    factory.registerFunction<FunctionNormalizeUTF8NFD>(documentation_nfd);
    factory.registerFunction<FunctionNormalizeUTF8NFKC>(documentation_nfkc);
    factory.registerFunction<FunctionNormalizeUTF8NFKD>(documentation_nfkd);
}

}

#endif
