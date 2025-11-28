#include <Functions/isDistinctFrom.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsLogical.h>


namespace DB
{

REGISTER_FUNCTION(IsDistinctFrom)
{
    FunctionDocumentation::Description description = R"(
        Performs a null-safe "not equals" comparison between two values.
        Returns `true` if the values are distinct (not equal), including when one value is NULL and the other is not.
        Returns `false` if the values are equal, or if both are NULL.
    )";

    FunctionDocumentation::Syntax syntax = "isDistinctFrom(x, y)";

    FunctionDocumentation::Arguments arguments = {
        {"x", "First value to compare. Can be any ClickHouse data type.", {"Any"}},
        {"y", "Second value to compare. Can be any ClickHouse data type.", {"Any"}}
    };

    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns `true` if the two values are different, treating NULLs as comparable:\n"
        "  - Returns `true` if x != y.\n"
        "  - Returns `true` if exactly one of x or y is NULL.\n"
        "  - Returns `false` if x = y, or both x and y are NULL.",
        {"Bool"}
    };


    FunctionDocumentation::Examples examples = {
        {"Basic usage with numbers and NULLs", R"(
SELECT
    isDistinctFrom(1, 2) AS result_1,
    isDistinctFrom(1, 1) AS result_2,
    isDistinctFrom(NULL, 1) AS result_3,
    isDistinctFrom(NULL, NULL) AS result_4
        )",
    R"(
┌─result_1─┬─result_2─┬─result_3─┬─result_4─┐
│        1 │        0 │        1 │        0 │
└──────────┴──────────┴──────────┴──────────┘
        )"}
    };

    FunctionDocumentation::IntroducedIn introduced_in = {25, 11};

    FunctionDocumentation::Category category = FunctionDocumentation::Category::Comparison;

    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionIsDistinctFrom>(documentation);
}

template <>
ColumnPtr FunctionComparison<NotEqualsOp, NameNotEquals, true /* is null safe cmp*/>::executeTupleImpl(
    const ColumnsWithTypeAndName & x, const ColumnsWithTypeAndName & y, size_t tuple_size, size_t input_rows_count) const
{
    FunctionOverloadResolverPtr func_builder_not_equals
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionIsDistinctFrom>(params));

    FunctionOverloadResolverPtr func_builder_and
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionOr>());

    return executeTupleEqualityImpl(
        func_builder_not_equals,
        func_builder_and,
        x, y, tuple_size, input_rows_count);
}

}
