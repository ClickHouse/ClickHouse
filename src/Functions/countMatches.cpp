#include <Core/Settings.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/Regexps.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace Setting
{
    extern const SettingsBool count_matches_stop_at_empty_match;
}

namespace
{

using Pos = const char *;

template <typename CountMatchesBase>
class FunctionCountMatches : public IFunction
{
    const bool count_matches_stop_at_empty_match;

public:
    static constexpr auto name = CountMatchesBase::name;
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionCountMatches<CountMatchesBase>>(context); }

    explicit FunctionCountMatches(ContextPtr context)
        : count_matches_stop_at_empty_match(context->getSettingsRef()[Setting::count_matches_stop_at_empty_match])
    {
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args
        {
            {"haystack", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String or FixedString"},
            {"pattern", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), isColumnConst, "constant String"}
        };
        validateFunctionArguments(*this, arguments, args);

        return std::make_shared<DataTypeUInt64>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const IColumn * col_pattern = arguments[1].column.get();
        const ColumnConst * col_pattern_const = checkAndGetColumnConst<ColumnString>(col_pattern);
        if (col_pattern_const == nullptr)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Pattern argument is not const");

        const OptimizedRegularExpression re = Regexps::createRegexp</*is_like*/ false, /*no_capture*/ true, CountMatchesBase::case_insensitive>(col_pattern_const->getValue<String>());

        const IColumn * col_haystack = arguments[0].column.get();
        OptimizedRegularExpression::MatchVec matches;

        if (const ColumnConst * col_haystack_const = checkAndGetColumnConstStringOrFixedString(col_haystack))
        {
            std::string_view str = col_haystack_const->getDataColumn().getDataAt(0).toView();
            uint64_t matches_count = countMatches(str, re, matches);
            return result_type->createColumnConst(input_rows_count, matches_count);
        }
        if (const ColumnString * col_haystack_string = checkAndGetColumn<ColumnString>(col_haystack))
        {
            auto col_res = ColumnUInt64::create();

            const ColumnString::Chars & src_chars = col_haystack_string->getChars();
            const ColumnString::Offsets & src_offsets = col_haystack_string->getOffsets();

            ColumnUInt64::Container & vec_res = col_res->getData();
            vec_res.resize(input_rows_count);

            ColumnString::Offset current_src_offset = 0;

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                Pos pos = reinterpret_cast<Pos>(&src_chars[current_src_offset]);
                current_src_offset = src_offsets[i];
                Pos end = reinterpret_cast<Pos>(&src_chars[current_src_offset]);

                std::string_view str(pos, end - pos);
                vec_res[i] = countMatches(str, re, matches);
            }

            return col_res;
        }
        if (const ColumnFixedString * col_haystack_fixedstring = checkAndGetColumn<ColumnFixedString>(col_haystack))
        {
            auto col_res = ColumnUInt64::create();

            ColumnUInt64::Container & vec_res = col_res->getData();
            vec_res.resize(input_rows_count);

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                std::string_view str = col_haystack_fixedstring->getDataAt(i).toView();
                vec_res[i] = countMatches(str, re, matches);
            }

            return col_res;
        }
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Could not cast haystack argument to String or FixedString");
    }

    uint64_t countMatches(std::string_view src, const OptimizedRegularExpression & re, OptimizedRegularExpression::MatchVec & matches) const
    {
        /// Only one match is required, no need to copy more.
        static const unsigned matches_limit = 1;

        Pos pos = reinterpret_cast<Pos>(src.data());
        Pos end = reinterpret_cast<Pos>(src.data() + src.size());

        uint64_t match_count = 0;
        while (pos < end)
        {
            if (re.match(pos, end - pos, matches, matches_limit))
            {
                if (matches[0].length > 0)
                {
                    pos += matches[0].offset + matches[0].length;
                    ++match_count;
                }
                else
                {
                    if (count_matches_stop_at_empty_match)
                        /// Progress should be made, but with empty match the progress will not be done.
                        break;

                    /// Progress is made by a single character in case the pattern does not match or have zero-byte match.
                    /// The reason is simply because the pattern could match another part of input when forwarded.
                    ++pos;
                }
            }
            else
                break;
        }

        return match_count;
    }
};

struct FunctionCountMatchesCaseSensitive
{
    static constexpr auto name = "countMatches";
    static constexpr bool case_insensitive = false;
};
struct FunctionCountMatchesCaseInsensitive
{
    static constexpr auto name = "countMatchesCaseInsensitive";
    static constexpr bool case_insensitive = true;
};

}

REGISTER_FUNCTION(CountMatches)
{
    FunctionDocumentation::Description description_case_sensitive = R"(
Returns number of matches of a regular expression in a string.

:::note Version dependent behavior
The behavior of this function depends on the ClickHouse version:

- in versions < v25.6, the function stops counting at the first empty match even if a pattern accepts.
- in versions >= 25.6, the function continues execution when an empty match occurs. The legacy behavior can be restored using setting `count_matches_stop_at_empty_match = true`;
:::

    )";
    FunctionDocumentation::Syntax syntax_case_sensitive = "countMatches(haystack, pattern)";
    FunctionDocumentation::Arguments arguments_case_sensitive = {
        {"haystack", "The string to search in.", {"String"}},
        {"pattern", "Regular expression pattern.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_case_sensitive = {"Returns the number of matches found.", {"UInt64"}};
    FunctionDocumentation::Examples examples_case_sensitive = {
    {
        "Count digit sequences",
        "SELECT countMatches('hello 123 world 456 test', '[0-9]+')",
        R"(
┌─countMatches('hello 123 world 456 test', '[0-9]+')─┐
│                                                   2 │
└─────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_case_sensitive = {21, 1};
    FunctionDocumentation::Category category_case_sensitive = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation_case_sensitive = {description_case_sensitive, syntax_case_sensitive, arguments_case_sensitive, returned_value_case_sensitive, examples_case_sensitive, introduced_in_case_sensitive, category_case_sensitive};

    FunctionDocumentation::Description description_case_insensitive = R"(
Like [`countMatches`](#countMatches) but performs case-insensitive matching.
    )";
    FunctionDocumentation::Syntax syntax_case_insensitive = "countMatchesCaseInsensitive(haystack, pattern)";
    FunctionDocumentation::Arguments arguments_case_insensitive = {
        {"haystack", "The string to search in.", {"String"}},
        {"pattern", "Regular expression pattern.", {"const String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_case_insensitive = {"Returns the number of matches found.", {"UInt64"}};
    FunctionDocumentation::Examples examples_case_insensitive = {
    {
        "Case insensitive count",
        "SELECT countMatchesCaseInsensitive('Hello HELLO world', 'hello')",
        R"(
┌─countMatchesCaseInsensitive('Hello HELLO world', 'hello')─┐
│                                                         2 │
└───────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_case_insensitive = {21, 1};
    FunctionDocumentation::Category category_case_insensitive = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation_case_insensitive = {description_case_insensitive, syntax_case_insensitive, arguments_case_insensitive, returned_value_case_insensitive, examples_case_insensitive, introduced_in_case_insensitive, category_case_insensitive};

    factory.registerFunction<FunctionCountMatches<FunctionCountMatchesCaseSensitive>>(documentation_case_sensitive);
    factory.registerFunction<FunctionCountMatches<FunctionCountMatchesCaseInsensitive>>(documentation_case_insensitive);
}

}
