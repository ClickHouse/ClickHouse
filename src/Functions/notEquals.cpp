#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsComparison.h>
#include <Functions/FunctionsLogical.h>

namespace DB
{

using FunctionNotEquals = FunctionComparison<NotEqualsOp, NameNotEquals>;

REGISTER_FUNCTION(NotEquals)
{
    // Documentation for notEquals
    FunctionDocumentation::Description description_notEquals = "Compares two values for inequality.";
    FunctionDocumentation::Syntax syntax_notEquals = R"(
    notEquals(a, b)
    -- a != b
    -- a <> b
)";
    FunctionDocumentation::Arguments arguments_notEquals = {
        {"a", "First value.<sup>[*](#comparison-rules)</sup>"},
        {"b", "Second value.<sup>[*](#comparison-rules)</sup>"}
    };
    FunctionDocumentation::ReturnedValue returned_value_notEquals = "Returns `1` if `a` is not equal to `b`, otherwise `0`. [`UInt8`](/sql-reference/data-types/int-uint/)";
    FunctionDocumentation::Examples examples_notEquals = {
        {"Usage example", "SELECT 1 != 2, 1 != 1;", R"(
┌─notEquals(1, 2)─┬─notEquals(1, 1)─┐
│               1 │               0 │
└─────────────────┴─────────────────┘
)"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_notEquals = {1, 1};
    FunctionDocumentation::Category category_notEquals = FunctionDocumentation::Category::Comparison;
    FunctionDocumentation documentation_notEquals = {
        description_notEquals,
        syntax_notEquals,
        arguments_notEquals,
        returned_value_notEquals,
        examples_notEquals,
        introduced_in_notEquals,
        category_notEquals
    };
    factory.registerFunction<FunctionNotEquals>(documentation_notEquals);
}

template <>
ColumnPtr FunctionComparison<NotEqualsOp, NameNotEquals>::executeTupleImpl(
    const ColumnsWithTypeAndName & x, const ColumnsWithTypeAndName & y, size_t tuple_size, size_t input_rows_count) const
{
    FunctionOverloadResolverPtr func_builder_not_equals
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionNotEquals>(params));

    FunctionOverloadResolverPtr func_builder_or
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionOr>());

    return executeTupleEqualityImpl(
        func_builder_not_equals,
        func_builder_or,
        x, y, tuple_size, input_rows_count);
}

}
