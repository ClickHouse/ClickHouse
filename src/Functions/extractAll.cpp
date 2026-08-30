#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionTokens.h>
#include <Functions/FunctionFactory.h>
#include <Functions/Regexps.h>
#include <Interpreters/Context.h>
#include <Interpreters/JIT/CompileRegexp.h>
#include <Common/StringUtils.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <limits>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}


/** Functions that split strings into an array of strings or vice versa.
  *
  * extractAll(s, regexp)     - select from the string the subsequences corresponding to the regexp.
  * - first subpattern, if regexp has subpattern;
  * - zero subpattern (the match part, otherwise);
  * - otherwise, an empty array
  */
namespace
{

using Pos = const char *;

class ExtractAllImpl
{
private:
    Regexps::RegexpPtr re;
    OptimizedRegularExpression::MatchVec matches;
    size_t capture{};

    /// Optional JIT-compiled matcher for simple patterns (see `CompileRegexp.h`).
    /// The capture arrays are sized once in `init` and reused for every row.
    RegexpJITMatcher matcher;
    VectorWithMemoryTracking<const uint8_t *> capture_starts;
    VectorWithMemoryTracking<const uint8_t *> capture_ends;

    Pos begin{};
    Pos pos{};
    Pos end{};
public:
    static constexpr auto name = "extractAll";
    static String getName() { return name; }
    static bool isVariadic() { return false; }
    static size_t getNumberOfArguments() { return 2; }

    static ColumnNumbers getArgumentsThatAreAlwaysConstant() { return {1}; }

    static void checkArguments(const IFunction & func, const ColumnsWithTypeAndName & arguments)
    {
        FunctionArgumentDescriptors mandatory_args{
            {"haystack", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"},
            {"pattern", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), isColumnConst, "const String"}
        };

        validateFunctionArguments(func, arguments, mandatory_args);
    }

    static constexpr auto strings_argument_position = 0uz;

    void init(
        const ColumnsWithTypeAndName & arguments,
        bool /*max_substrings_includes_remaining_string*/,
        size_t regexp_jit_min_count = std::numeric_limits<size_t>::max())
    {
        const ColumnConst * col = checkAndGetColumnConstStringOrFixedString(arguments[1].column.get());

        if (!col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}. "
                "Must be constant string.", arguments[1].column->getName(), getName());

        const String pattern = col->getValue<String>();
        re = std::make_shared<OptimizedRegularExpression>(Regexps::createRegexp<false, false, false>(pattern));
        capture = re->getNumberOfSubpatterns() > 0 ? 1 : 0;

        matches.resize(capture + 1);

        /// `extractAll` builds RE2 with `RE_DOT_NL`, so `.` matches newline.
        matcher = getRegexpJITMatcher(pattern, /* case_insensitive */ false, /* dot_all */ true, regexp_jit_min_count);
        if (matcher)
        {
            capture_starts.resize(matcher.num_captures);
            capture_ends.resize(matcher.num_captures);
        }
    }

    /// Called for each next string.
    void set(Pos pos_, Pos end_)
    {
        begin = pos_;
        pos = pos_;
        end = end_;
    }

    /// Get the next token, if any, or return false.
    bool get(Pos & token_begin, Pos & token_end)
    {
        if (!pos || pos > end)
            return false;

        if (matcher)
        {
            /// Search for the leftmost match at or after `pos`, but keep the whole string `[begin, end)` as the
            /// subject so that `^` stays anchored at the true string start (as in the RE2 path below) instead of
            /// re-anchoring at every iteration. The matcher reports capture spans as absolute pointers into the
            /// haystack (see `CompileRegexp.h`).
            const auto * subject_begin = reinterpret_cast<const uint8_t *>(begin);
            const auto * subject_end = reinterpret_cast<const uint8_t *>(end);
            const auto * search_from = reinterpret_cast<const uint8_t *>(pos);
            if (matcher.func(subject_begin, subject_end, search_from, capture_starts.data(), capture_ends.data()) != 1)
                return false;
            if (capture_ends[0] == capture_starts[0]) /// empty whole match - stop, like the RE2 path
                return false;

            if (capture_starts[capture] == nullptr)
            {
                token_begin = pos;
                token_end = pos;
            }
            else
            {
                token_begin = reinterpret_cast<Pos>(capture_starts[capture]);
                token_end = reinterpret_cast<Pos>(capture_ends[capture]);
            }
            pos = reinterpret_cast<Pos>(capture_ends[0]);
            return true;
        }

        /// Match over the whole string starting at `pos`, so that the characters before `pos` are seen as context
        /// for zero-width assertions such as `\b` and `^`. The returned offsets are relative to `begin`.
        if (!re->match(begin, end - begin, pos - begin, matches) || !matches[0].length)
            return false;

        if (matches[capture].offset == std::string::npos)
        {
            /// Empty match.
            token_begin = pos;
            token_end = pos;
        }
        else
        {
            token_begin = begin + matches[capture].offset;
            token_end = token_begin + matches[capture].length;
        }

        pos = begin + matches[0].offset + matches[0].length;

        return true;
    }
};

using FunctionExtractAll = FunctionTokens<ExtractAllImpl>;

}

REGISTER_FUNCTION(ExtractAll)
{
    FunctionDocumentation::Description description = R"(
Like [`extract`](#extract), but returns an array of all matches of a regular expression in a string.
If 'haystack' doesn't match the 'pattern' regex, an empty array is returned.

If the regular expression has capturing groups (sub-patterns), the function matches the input string against the first capturing group.
    )";
    FunctionDocumentation::Syntax syntax = "extractAll(haystack, pattern)";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "String from which to extract fragments.", {"String"}},
        {"pattern", "Regular expression, optionally containing capturing groups.", {"const String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns array of extracted fragments.", {"Array(String)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Extract all numbers",
        "SELECT extractAll('hello 123 world 456', '[0-9]+')",
        R"(
┌─extractAll('hello 123 world 456', '[0-9]+')─┐
│ ['123','456']                               │
└─────────────────────────────────────────────┘
        )"
    },
    {
        "Extract using capturing group",
        "SELECT extractAll('test@example.com, user@domain.org', '([a-zA-Z0-9]+)@')",
        R"(
┌─extractAll('test@example.com, user@domain.org', '([a-zA-Z0-9]+)@')─┐
│ ['test','user']                                                    │
└────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionExtractAll>(documentation);
}

}
