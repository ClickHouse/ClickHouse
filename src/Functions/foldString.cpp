#include "config.h"

#if USE_ICU

#include <Columns/ColumnString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <unicode/uchar.h>
#include <unicode/unorm2.h>
#include <unicode/ustring.h>
#include <unicode/utypes.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int CANNOT_NORMALIZE_STRING;
}

namespace
{

/// Maximum expansion factors for UTF-16 normalization/folding operations.
/// See https://unicode.org/faq/normalization.html#12
constexpr int MAX_NFC_EXPANSION = 3;
constexpr int MAX_NFD_EXPANSION = 4;

/// Case folding can also expand (e.g. `ﬃ` → `ffi`). See https://unicode.org/Public/UCD/latest/ucd/CaseFolding.txt
constexpr int MAX_CASEFOLD_EXPANSION = 3;

/// Each UTF-16 code unit produces at most 3 UTF-8 bytes.
/// Chars which require 4 UTF-8 bytes also require 2 UTF-16 code units, so the max expansion factor is 3.
constexpr int MAX_UTF16_TO_UTF8_EXPANSION = 3;

struct CaseFoldImpl
{
    static constexpr auto name = "caseFoldUTF8";

    static void init(const UNormalizer2 *& /*nfc*/, const UNormalizer2 *& /*nfd*/) {}

    static int32_t transform(
        const UNormalizer2 * /*nfc*/, const UNormalizer2 * /*nfd*/,
        PODArray<UChar> & buf_in, PODArray<UChar> & buf_out, int32_t u16_len)
    {
        buf_out.resize(u16_len * MAX_CASEFOLD_EXPANSION);
        UErrorCode err = U_ZERO_ERROR;
        int32_t len = u_strFoldCase(buf_out.data(), static_cast<int32_t>(buf_out.size()),
            buf_in.data(), u16_len, U_FOLD_CASE_DEFAULT, &err);
        if (U_FAILURE(err))
            throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Case fold failed (u_strFoldCase): {}", u_errorName(err));

        return len;
    }
};

struct RemoveDiacriticsImpl
{
    static constexpr auto name = "removeDiacriticsUTF8";

    static void init(const UNormalizer2 *& nfc, const UNormalizer2 *& nfd)
    {
        UErrorCode err = U_ZERO_ERROR;
        nfc = unorm2_getNFCInstance(&err);
        if (U_FAILURE(err))
            throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Failed to get NFC normalizer: {}", u_errorName(err));

        err = U_ZERO_ERROR;
        nfd = unorm2_getNFDInstance(&err);
        if (U_FAILURE(err))
            throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Failed to get NFD normalizer: {}", u_errorName(err));
    }

    static int32_t transform(
        const UNormalizer2 * nfc, const UNormalizer2 * nfd,
        PODArray<UChar> & buf_in, PODArray<UChar> & buf_out, int32_t u16_len)
    {
        /// NFD decompose
        buf_out.resize(u16_len * MAX_NFD_EXPANSION);
        UErrorCode err = U_ZERO_ERROR;
        int32_t len = unorm2_normalize(nfd, buf_in.data(), u16_len, buf_out.data(), static_cast<int32_t>(buf_out.size()), &err);
        if (U_FAILURE(err))
            throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Accent fold failed (NFD): {}", u_errorName(err));
        std::swap(buf_in, buf_out);

        /// Strip combining marks
        int32_t read_pos = 0;
        while (read_pos < len)
        {
            UChar32 code_point;
            int32_t prev = read_pos;
            U16_NEXT(buf_in.data(), read_pos, len, code_point); /// advances read_pos to next code point boundary

            if (u_charType(code_point) == U_NON_SPACING_MARK)
            {
                int32_t write_pos = prev;
                while (read_pos < len)
                {
                    prev = read_pos;
                    U16_NEXT(buf_in.data(), read_pos, len, code_point);
                    if (u_charType(code_point) != U_NON_SPACING_MARK)
                    {
                        for (int32_t i = prev; i < read_pos; ++i)
                            buf_in.data()[write_pos++] = buf_in.data()[i];
                    }
                }
                len = write_pos;
            }
        }

        /// NFC recompose
        buf_out.resize(len * MAX_NFC_EXPANSION);
        err = U_ZERO_ERROR;
        len = unorm2_normalize(nfc, buf_in.data(), len, buf_out.data(), static_cast<int32_t>(buf_out.size()), &err);
        if (U_FAILURE(err))
            throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Accent fold failed (NFC recompose): {}", u_errorName(err));

        return len;
    }
};

/// Common row-loop template: handles UTF-8 ↔ UTF-16 conversion and per-row transform.
template <typename Impl>
struct FoldUTF8Impl
{
    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        const UNormalizer2 * nfc = nullptr;
        const UNormalizer2 * nfd = nullptr;
        Impl::init(nfc, nfd);

        res_data.reserve(data.size());
        res_offsets.resize(input_rows_count);

        ColumnString::Offset current_from_offset = 0;
        ColumnString::Offset current_to_offset = 0;

        PODArray<UChar> buf_in;
        PODArray<UChar> buf_out;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            size_t from_size = offsets[i] - current_from_offset;

            if (from_size > 0)
            {
                /// UTF-8 → UTF-16
                buf_in.resize(from_size);
                int32_t u16_len = 0;
                UErrorCode err = U_ZERO_ERROR;
                u_strFromUTF8(
                    buf_in.data(),
                    static_cast<int32_t>(buf_in.size()),
                    &u16_len,
                    reinterpret_cast<const char *>(&data[current_from_offset]),
                    static_cast<int32_t>(from_size),
                    &err);
                if (U_FAILURE(err))
                    throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "{} failed (strFromUTF8): {}", Impl::name, u_errorName(err));

                /// Impl-specific transform pipeline; result is left in buf_out
                int32_t len = Impl::transform(nfc, nfd, buf_in, buf_out, u16_len);

                size_t max_to_size = current_to_offset + MAX_UTF16_TO_UTF8_EXPANSION * static_cast<size_t>(len);
                if (res_data.size() < max_to_size)
                    res_data.resize(max_to_size);

                /// UTF-16 → UTF-8
                int32_t to_size = 0;
                err = U_ZERO_ERROR;
                u_strToUTF8(
                    reinterpret_cast<char *>(&res_data[current_to_offset]),
                    static_cast<int32_t>(res_data.size() - current_to_offset),
                    &to_size,
                    buf_out.data(),
                    len,
                    &err);
                if (U_FAILURE(err))
                    throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "{} failed (strToUTF8): {}", Impl::name, u_errorName(err));

                current_to_offset += to_size;
            }

            res_offsets[i] = current_to_offset;
            current_from_offset = offsets[i];
        }

        res_data.resize(current_to_offset);
    }

    [[noreturn]] static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &, size_t)
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot apply function {} to fixed string.", Impl::name);
    }
};

using FunctionCaseFoldUTF8 = FunctionStringToString<FoldUTF8Impl<CaseFoldImpl>, CaseFoldImpl>;
using FunctionRemoveDiacriticsUTF8 = FunctionStringToString<FoldUTF8Impl<RemoveDiacriticsImpl>, RemoveDiacriticsImpl>;

}

REGISTER_FUNCTION(FoldUTF8)
{
    FunctionDocumentation::IntroducedIn intro = {26, 3};
    FunctionDocumentation::Category cat = FunctionDocumentation::Category::String;

    /// caseFoldUTF8
    FunctionDocumentation::Description case_desc = R"(
Applies Unicode case folding to a UTF-8 string, converting it to a lowercase-like normalized form suitable for case-insensitive comparisons.

Applies standard Unicode case folding. Preserves compatibility characters that are not affected by case folding
(e.g. Roman numerals, circled numbers), but note that some ligatures like `ﬃ` are still decomposed because Unicode case folding itself expands them.
)";
    FunctionDocumentation::Syntax case_syntax = "caseFoldUTF8(str)";
    FunctionDocumentation::Arguments case_args = {
        {"str", "UTF-8 encoded input string.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue case_ret = {"Case-folded UTF-8 string.", {"String"}};
    FunctionDocumentation::Examples case_examples = {
    {
        "Basic case folding",
        "SELECT caseFoldUTF8('Straße')",
        R"(
┌─caseFoldUTF8('Straße')─┐
│ strasse                 │
└─────────────────────────┘
)"
    }};
    factory.registerFunction<FunctionCaseFoldUTF8>({case_desc, case_syntax, case_args, {}, case_ret, case_examples, intro, cat});

    /// removeDiacriticsUTF8
    FunctionDocumentation::Description accent_desc = R"(
Removes diacritical marks (accents) from a UTF-8 string by decomposing characters via NFD,
stripping combining marks (Unicode category Mn), then recomposing via NFC.
)";
    FunctionDocumentation::Syntax accent_syntax = "removeDiacriticsUTF8(str)";
    FunctionDocumentation::Arguments accent_args = {
        {"str", "UTF-8 encoded input string.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue accent_ret = {"UTF-8 string with diacritics removed.", {"String"}};
    FunctionDocumentation::Examples accent_examples = {{
        "Basic accent removal",
        "SELECT removeDiacriticsUTF8('café résumé naïve')",
        R"(
┌─removeDiacriticsUTF8('café résumé naïve')─┐
│ cafe resume naive                          │
└────────────────────────────────────────────┘
)"
    }};
    factory.registerFunction<FunctionRemoveDiacriticsUTF8>({accent_desc, accent_syntax, accent_args, {}, accent_ret, accent_examples, intro, cat});
    factory.registerAlias("removeAccentsUTF8", "removeDiacriticsUTF8");
}

}

#endif
