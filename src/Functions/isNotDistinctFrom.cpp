#include <Functions/isNotDistinctFrom.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsLogical.h>

namespace DB
{

REGISTER_FUNCTION(IsNotDistinctFrom)
{
    FunctionDocumentation::Description description = R"(
        Performs a null-safe "equals" comparison between two values.
        Returns `true` if the values are equal, including when both are NULL.
        Returns `false` if the values are different, or if exactly one of them is NULL.
    )";

    FunctionDocumentation::Syntax syntax = "isNotDistinctFrom(x, y)";

    FunctionDocumentation::Arguments arguments = {
        {"x", "First value to compare. Can be any ClickHouse data type.", {"Any"}},
        {"y", "Second value to compare. Can be any ClickHouse data type.", {"Any"}}
    };

    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns `true` if the two values are equal, treating NULLs as comparable:\n"
        "  - Returns `true` if x = y.\n"
        "  - Returns `true` if both x and y are NULL.\n"
        "  - Returns `false` if x != y, or exactly one of x or y is NULL.",
        {"Bool"}
    };

    FunctionDocumentation::Examples examples = {
        {"Basic usage with numbers and NULLs", R"(
SELECT
    isNotDistinctFrom(1, 1) AS result_1,
    isNotDistinctFrom(1, 2) AS result_2,
    isNotDistinctFrom(NULL, NULL) AS result_3,
    isNotDistinctFrom(NULL, 1) AS result_4
        )",
    R"(
┌─result_1─┬─result_2─┬─result_3─┬─result_4─┐
│        1 │        0 │        1 │        0 │
└──────────┴──────────┴──────────┴──────────┘
        )"}
    };

    FunctionDocumentation::IntroducedIn introduced_in = {25, 10};

    FunctionDocumentation::Category category = FunctionDocumentation::Category::Comparison;

    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionIsNotDistinctFrom>(documentation);
}

template <>
ColumnPtr FunctionComparison<EqualsOp, NameEquals, true /* is null safe cmp*/>::executeTupleImpl(
    const ColumnsWithTypeAndName & x, const ColumnsWithTypeAndName & y, size_t tuple_size, size_t input_rows_count) const
{
    FunctionOverloadResolverPtr func_builder_equals
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionIsNotDistinctFrom>(params));

    FunctionOverloadResolverPtr func_builder_and
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());

    return executeTupleEqualityImpl(
        func_builder_equals,
        func_builder_and,
        x, y, tuple_size, input_rows_count);
}

}
