#include <Functions/FunctionsStringSearchToString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/Regexps.h>
#include <Common/OptimizedRegularExpression.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/isValidUTF8.h>
#include <Interpreters/JIT/CompileRegexp.h>

#include <cstring>
#include <limits>


namespace DB
{
namespace
{

struct ExtractImpl
{
#ifndef NDEBUG
    /// Assert that the JIT-compiled extraction agrees with RE2 for every row. Debug builds only.
    static void verifyExtractAgainstRE2(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        const std::string & pattern,
        const ColumnString::Chars & res_data,
        const ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        const OptimizedRegularExpression regexp = Regexps::createRegexp<false, false, false>(pattern);
        unsigned capture = regexp.getNumberOfSubpatterns() > 0 ? 1 : 0;
        OptimizedRegularExpression::MatchVec matches;
        size_t prev_offset = 0;
        size_t res_prev_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const char * d = reinterpret_cast<const char *>(&data[prev_offset]);
            const size_t size = offsets[i] - prev_offset;

            /// Byte-wise matching only matches RE2 (UTF-8 mode) on valid UTF-8; invalid UTF-8 may differ.
            if (UTF8::isValidUTF8(&data[prev_offset], size))
            {
                unsigned count = regexp.match(d, size, matches, capture + 1);

                const char * expected_ptr = nullptr;
                size_t expected_len = 0;
                if (count > capture && matches[capture].offset != std::string::npos)
                {
                    expected_ptr = d + matches[capture].offset;
                    expected_len = matches[capture].length;
                }

                const size_t jit_len = res_offsets[i] - res_prev_offset;
                chassert(jit_len == expected_len);
                chassert(jit_len == 0 || 0 == memcmp(&res_data[res_prev_offset], expected_ptr, expected_len));
            }

            res_prev_offset = res_offsets[i];
            prev_offset = offsets[i];
        }
    }
#endif

    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        const std::string & pattern,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count,
        size_t regexp_jit_min_count = std::numeric_limits<size_t>::max())
    {
        res_data.reserve(data.size() / 5);
        res_offsets.resize(input_rows_count);

        /// Fast path: a JIT-compiled matcher for a simple regular expression (see `CompileRegexp.h`).
        if (regexp_jit_min_count != std::numeric_limits<size_t>::max())
        {
            /// `extract` builds RE2 with `RE_DOT_NL` (see `Regexps::createRegexp`), so `.` matches newline.
            if (auto matcher = getRegexpJITMatcher(pattern, /* case_insensitive */ false, /* dot_all */ true, regexp_jit_min_count))
            {
                /// Extract the first capturing group, or the whole match if there is none.
                const int group = matcher.num_captures > 1 ? 1 : 0;
                /// Allocated once per call (not per row); reused for every row below.
                VectorWithMemoryTracking<const uint8_t *> capture_starts(matcher.num_captures);
                VectorWithMemoryTracking<const uint8_t *> capture_ends(matcher.num_captures);

                size_t prev_offset = 0;
                size_t res_offset = 0;
                for (size_t i = 0; i < input_rows_count; ++i)
                {
                    const auto * str_begin = reinterpret_cast<const uint8_t *>(data.data() + prev_offset);
                    const auto * str_end = reinterpret_cast<const uint8_t *>(data.data() + offsets[i]);

                    const uint8_t * src = nullptr;
                    size_t length = 0;
                    if (matcher.func(str_begin, str_end, str_begin, capture_starts.data(), capture_ends.data()) == 1
                        && capture_starts[group] != nullptr && capture_ends[group] != nullptr)
                    {
                        src = capture_starts[group];
                        length = capture_ends[group] - capture_starts[group];
                    }

                    if (src != nullptr)
                    {
                        res_data.resize(res_offset + length);
                        if (length)
                            memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], src, length);
                        res_offset += length;
                    }
                    else
                    {
                        res_data.resize(res_offset);
                    }

                    res_offsets[i] = res_offset;
                    prev_offset = offsets[i];
                }

#ifndef NDEBUG
                verifyExtractAgainstRE2(data, offsets, pattern, res_data, res_offsets, input_rows_count);
#endif
                return;
            }
        }

        const OptimizedRegularExpression regexp = Regexps::createRegexp<false, false, false>(pattern);

        unsigned capture = regexp.getNumberOfSubpatterns() > 0 ? 1 : 0;
        OptimizedRegularExpression::MatchVec matches;
        matches.reserve(capture + 1);
        size_t prev_offset = 0;
        size_t res_offset = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            size_t cur_offset = offsets[i];

            unsigned count
                = regexp.match(reinterpret_cast<const char *>(&data[prev_offset]), cur_offset - prev_offset, matches, capture + 1);
            if (count > capture && matches[capture].offset != std::string::npos)
            {
                const auto & match = matches[capture];
                res_data.resize(res_offset + match.length);
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], &data[prev_offset + match.offset], match.length);
                res_offset += match.length;
            }
            else
            {
                res_data.resize(res_offset);
            }

            res_offsets[i] = res_offset;
            prev_offset = cur_offset;
        }
    }
};

struct NameExtract
{
    static constexpr auto name = "extract";
};

using FunctionExtract = FunctionsStringSearchToString<ExtractImpl, NameExtract>;

}

REGISTER_FUNCTION(Extract)
{
    FunctionDocumentation::Description description = R"(
Extracts the first match of a regular expression in a string.
If 'haystack' doesn't match 'pattern', an empty string is returned.

This function uses the RE2 regular expression library. Please refer to [re2](https://github.com/google/re2/wiki/Syntax) for supported syntax.

If the regular expression has capturing groups (sub-patterns), the function matches the input string against the first capturing group.
    )";
    FunctionDocumentation::Syntax syntax = "extract(haystack, pattern)";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "String from which to extract.", {"String"}},
        {"pattern", "Regular expression, typically containing a capturing group.", {"const String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns extracted fragment as a string.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Extract domain from email",
        "SELECT extract('test@clickhouse.com', '.*@(.*)$')",
        R"(
┌─extract('test@clickhouse.com', '.*@(.*)$')─┐
│ clickhouse.com                            │
└───────────────────────────────────────────┘
        )"
    },
    {
        "No match returns empty string",
        "SELECT extract('test@clickhouse.com', 'no_match')",
        R"(
┌─extract('test@clickhouse.com', 'no_match')─┐
│                                            │
└────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionExtract>(documentation);
}

}
