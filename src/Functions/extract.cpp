#include <Functions/FunctionsStringSearchToString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/Regexps.h>
#include <Common/OptimizedRegularExpression.h>


namespace DB
{
namespace
{

struct ExtractImpl
{
    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        const std::string & pattern,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        res_data.reserve(data.size() / 5);
        res_offsets.resize(input_rows_count);

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
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionExtract>(documentation);
}

}
