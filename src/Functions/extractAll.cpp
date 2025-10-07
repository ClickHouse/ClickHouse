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
#include <Common/StringUtils.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>


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
    size_t capture;

    Pos pos;
    Pos end;
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

    void init(const ColumnsWithTypeAndName & arguments, bool /*max_substrings_includes_remaining_string*/)
    {
        const ColumnConst * col = checkAndGetColumnConstStringOrFixedString(arguments[1].column.get());

        if (!col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}. "
                "Must be constant string.", arguments[1].column->getName(), getName());

        re = std::make_shared<OptimizedRegularExpression>(Regexps::createRegexp<false, false, false>(col->getValue<String>()));
        capture = re->getNumberOfSubpatterns() > 0 ? 1 : 0;

        matches.resize(capture + 1);
    }

    /// Called for each next string.
    void set(Pos pos_, Pos end_)
    {
        pos = pos_;
        end = end_;
    }

    /// Get the next token, if any, or return false.
    bool get(Pos & token_begin, Pos & token_end)
    {
        if (!pos || pos > end)
            return false;

        if (!re->match(pos, end - pos, matches) || !matches[0].length)
            return false;

        if (matches[capture].offset == std::string::npos)
        {
            /// Empty match.
            token_begin = pos;
            token_end = pos;
        }
        else
        {
            token_begin = pos + matches[capture].offset;
            token_end = token_begin + matches[capture].length;
        }

        pos += matches[0].offset + matches[0].length;

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
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionExtractAll>(documentation);
}

}
