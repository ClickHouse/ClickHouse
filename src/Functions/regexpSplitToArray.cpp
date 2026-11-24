#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/FunctionDocumentation.h>

namespace DB
{

namespace
{

/// `regexp_split_to_array(string, pattern)` - PostgreSQL compatibility function.
/// It is `splitByRegexp(pattern, string)` with the arguments in the order PostgreSQL has them, as the issue
/// that asked for it defines it; it does not reimplement the rules of PostgreSQL for a match of an empty
/// string, which `splitByRegexp` does not split at. The arguments
/// are passed to `splitByRegexp` as they are, so the checks of the arguments, the handling of `NULL` and the
/// result are the ones of `splitByRegexp`: this function opts out of the default implementations that would
/// otherwise be applied a second time.
class FunctionRegexpSplitToArray final : public IFunction
{
public:
    static constexpr auto name = "regexp_split_to_array";

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionRegexpSplitToArray>(FunctionFactory::instance().get("splitByRegexp", context));
    }

    explicit FunctionRegexpSplitToArray(FunctionOverloadResolverPtr split_by_regexp_) : split_by_regexp(std::move(split_by_regexp_)) {}

    [[nodiscard]] String getName() const override { return name; }
    [[nodiscard]] size_t getNumberOfArguments() const override { return 2; }
    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    [[nodiscard]] bool useDefaultImplementationForNulls() const override { return false; }
    [[nodiscard]] bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    [[nodiscard]] DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        return split_by_regexp->build(swapArguments(arguments))->getResultType();
    }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnsWithTypeAndName swapped_arguments = swapArguments(arguments);
        auto split_by_regexp_base = split_by_regexp->build(swapped_arguments);
        return split_by_regexp_base->execute(
            swapped_arguments, split_by_regexp_base->getResultType(), input_rows_count, /* dry_run = */ false);
    }

private:
    static ColumnsWithTypeAndName swapArguments(const ColumnsWithTypeAndName & arguments)
    {
        return {arguments[1], arguments[0]};
    }

    FunctionOverloadResolverPtr split_by_regexp;
};

}

REGISTER_FUNCTION(RegexpSplitToArray)
{
    FunctionDocumentation::Description description = R"(
PostgreSQL compatibility function. Splits a string at the matches of a regular expression into an array of substrings.

It is [`splitByRegexp`](/reference/functions/regular-functions/splitting-merging-functions#splitByRegexp) with the arguments in the order PostgreSQL has them: `regexp_split_to_array(s, regexp)` is `splitByRegexp(regexp, s)`.
The result is the one of `splitByRegexp`, which differs from PostgreSQL in these cases:
- The regular expression uses the re2 syntax of ClickHouse, not the syntax of PostgreSQL, and the optional `flags` argument of PostgreSQL is not supported.
- A match of an empty string does not split the string: `regexp_split_to_array('abc', 'x*')` is `['abc']`, while PostgreSQL splits it into characters.
- An empty regular expression splits the string into bytes, not into characters.
)";
    FunctionDocumentation::Syntax syntax = "regexp_split_to_array(s, regexp)";
    FunctionDocumentation::Arguments arguments = {
        {"s", "The string to split.", {"String"}},
        {"regexp", "Regular expression. Constant.", {"String", "FixedString"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an array of the substrings of `s` between the matches of `regexp`.", {"Array(String)"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", "SELECT regexp_split_to_array('a1b2c', '[0-9]')", "['a','b','c']"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 12};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSplitting;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionRegexpSplitToArray>(documentation, FunctionFactory::Case::Insensitive);
}

}
