#include <limits>
#include <string_view>
#include <Columns/ColumnString.h>
#include <Common/RegexpUtils.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsStringSearch.h>
#include <Functions/MatchImpl.h>


namespace DB
{
namespace
{

struct NameFullMatch
{
    static constexpr auto name = "fullMatch";
};

using UnderlyingMatchImpl = MatchImpl<NameFullMatch, MatchTraits::Syntax::Re2, MatchTraits::Case::Sensitive, MatchTraits::Result::DontNegate>;

/// Like `match` but the pattern must match the whole haystack, not a substring of it.
/// Implemented by anchoring the pattern at both ends and delegating to MatchImpl,
/// so all its fast paths (pattern analysis, substring prefilter, JIT) are reused.
struct FullMatchImpl
{
    static constexpr bool use_default_implementation_for_constants = UnderlyingMatchImpl::use_default_implementation_for_constants;
    static constexpr bool supports_start_pos = UnderlyingMatchImpl::supports_start_pos;
    static constexpr auto name = NameFullMatch::name;

    static ColumnNumbers getArgumentsThatAreAlwaysConstant() { return UnderlyingMatchImpl::getArgumentsThatAreAlwaysConstant(); }

    using ResultType = UnderlyingMatchImpl::ResultType;

    static ColumnString::MutablePtr anchorPatterns(const ColumnString::Chars & needle_data, const ColumnString::Offsets & needle_offsets)
    {
        auto anchored_needles = ColumnString::create();
        anchored_needles->reserve(needle_offsets.size());
        size_t prev_offset = 0;
        for (size_t i = 0; i < needle_offsets.size(); ++i)
        {
            const std::string_view needle(reinterpret_cast<const char *>(&needle_data[prev_offset]), needle_offsets[i] - prev_offset);
            const String anchored_needle = anchorRegexpToMatchWholeString(needle);
            anchored_needles->insertData(anchored_needle.data(), anchored_needle.size());
            prev_offset = needle_offsets[i];
        }
        return anchored_needles;
    }

    static void vectorConstant(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const String & needle,
        const ColumnPtr & start_pos_,
        PaddedPODArray<UInt8> & res,
        ColumnUInt8 * res_null,
        size_t input_rows_count,
        size_t regexp_jit_min_count = std::numeric_limits<size_t>::max())
    {
        UnderlyingMatchImpl::vectorConstant(
            haystack_data,
            haystack_offsets,
            anchorRegexpToMatchWholeString(needle),
            start_pos_,
            res,
            res_null,
            input_rows_count,
            regexp_jit_min_count);
    }

    static void vectorFixedConstant(
        const ColumnString::Chars & haystack,
        size_t N,
        const String & needle,
        PaddedPODArray<UInt8> & res,
        ColumnUInt8 * res_null,
        size_t input_rows_count)
    {
        UnderlyingMatchImpl::vectorFixedConstant(haystack, N, anchorRegexpToMatchWholeString(needle), res, res_null, input_rows_count);
    }

    static void vectorVector(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const ColumnString::Chars & needle_data,
        const ColumnString::Offsets & needle_offsets,
        const ColumnPtr & start_pos_,
        PaddedPODArray<UInt8> & res,
        ColumnUInt8 * res_null,
        size_t input_rows_count)
    {
        const auto anchored_needles = anchorPatterns(needle_data, needle_offsets);
        UnderlyingMatchImpl::vectorVector(
            haystack_data,
            haystack_offsets,
            anchored_needles->getChars(),
            anchored_needles->getOffsets(),
            start_pos_,
            res,
            res_null,
            input_rows_count);
    }

    static void vectorFixedVector(
        const ColumnString::Chars & haystack,
        size_t N,
        const ColumnString::Chars & needle_data,
        const ColumnString::Offsets & needle_offsets,
        const ColumnPtr & start_pos_,
        PaddedPODArray<UInt8> & res,
        ColumnUInt8 * res_null,
        size_t input_rows_count)
    {
        const auto anchored_needles = anchorPatterns(needle_data, needle_offsets);
        UnderlyingMatchImpl::vectorFixedVector(
            haystack, N, anchored_needles->getChars(), anchored_needles->getOffsets(), start_pos_, res, res_null, input_rows_count);
    }

    static void constantVector(
        const String & haystack,
        const ColumnString::Chars & needle_data,
        const ColumnString::Offsets & needle_offsets,
        const ColumnPtr & start_pos_,
        PaddedPODArray<UInt8> & res,
        ColumnUInt8 * res_null,
        size_t input_rows_count)
    {
        const auto anchored_needles = anchorPatterns(needle_data, needle_offsets);
        UnderlyingMatchImpl::constantVector(
            haystack, anchored_needles->getChars(), anchored_needles->getOffsets(), start_pos_, res, res_null, input_rows_count);
    }
};

using FunctionFullMatch = FunctionsStringSearch<FullMatchImpl>;

}

REGISTER_FUNCTION(FullMatch)
{
    FunctionDocumentation::Description description = R"(
Checks if a provided string matches the provided regular expression pattern as a whole.

Unlike [`match`](#match), which searches for a match anywhere in the string, this function
returns `1` only if the pattern matches the entire string, as if the pattern was anchored at
both ends with `^` and `$`. This corresponds to `RE2::FullMatch` of the RE2 library, to
`re.fullmatch` in Python, and to `regexp_full_match` in DuckDB.

This function uses the RE2 regular expression library. Please refer to [re2](https://github.com/google/re2/wiki/Syntax) for supported syntax.

Matching works under UTF-8 assumptions, e.g. `¥` uses two bytes internally but matching treats it as a single codepoint.
The regular expression must not contain NULL bytes.
If the haystack or the pattern are not valid UTF-8, the behavior is undefined.

Unlike re2's default behavior, `.` matches line breaks. To disable this, prepend the pattern with `(?-s)`.
    )";
    FunctionDocumentation::Syntax syntax = "fullMatch(haystack, pattern)";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "String in which the pattern is searched.", {"String"}},
        {"pattern", "Regular expression pattern. Can be a constant or come from a column.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if the pattern matches the whole string, `0` otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Matching the whole string",
        "SELECT fullMatch('abcde', 'a.*e'), fullMatch('abcde', 'b.*d'), match('abcde', 'b.*d')",
        R"(
┌─fullMatch('abcde', 'a.*e')─┬─fullMatch('abcde', 'b.*d')─┬─match('abcde', 'b.*d')─┐
│                          1 │                          0 │                      1 │
└────────────────────────────┴────────────────────────────┴────────────────────────┘
        )"
    },
    {
        "Pattern with a top-level alternation",
        "SELECT fullMatch('gold-1', 'zzz|gold.*'), fullMatch('app-gold-1', 'zzz|gold.*')",
        R"(
┌─fullMatch('gold-1', 'zzz|gold.*')─┬─fullMatch('app-gold-1', 'zzz|gold.*')─┐
│                                 1 │                                     0 │
└───────────────────────────────────┴───────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 11};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionFullMatch>(documentation);
    factory.registerAlias("regexp_full_match", NameFullMatch::name, FunctionFactory::Case::Insensitive);
}

}
