#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsComparison.h>
#include <Functions/FunctionsLogical.h>

namespace DB
{

using FunctionGreaterOrEquals = FunctionComparison<GreaterOrEqualsOp, NameGreaterOrEquals>;
using FunctionGreater = FunctionComparison<GreaterOp, NameGreater>;
extern template class FunctionComparison<GreaterOp, NameGreater>;
using FunctionEquals = FunctionComparison<EqualsOp, NameEquals>;
extern template class FunctionComparison<EqualsOp, NameEquals>;

REGISTER_FUNCTION(GreaterOrEquals)
{
    // Documentation for greaterOrEquals
    FunctionDocumentation::Description description_greaterOrEquals = "Compares two values for greater-than-or-equal-to relation.";
    FunctionDocumentation::Syntax syntax_greaterOrEquals = R"(
    greaterOrEquals(a, b)
    -- a >= b
)";
    FunctionDocumentation::Arguments arguments_greaterOrEquals = {
        {"a", "First value.<sup>[*](#comparison-rules)</sup>"},
        {"b", "Second value.<sup>[*](#comparison-rules)</sup>"}
    };
    FunctionDocumentation::ReturnedValue returned_value_greaterOrEquals = "Returns `1` if `a` is greater than or equal to `b`, otherwise `0`. [`UInt8`](/sql-reference/data-types/int-uint/)";
    FunctionDocumentation::Examples examples_greaterOrEquals = {
        {"Usage example", "SELECT 2 >= 1, 2 >= 2, 1 >= 2;", R"(
┌─greaterOrEquals(2, 1)─┬─greaterOrEquals(2, 2)─┬─greaterOrEquals(1, 2)─┐
│                     1 │                     1 │                     0 │
└───────────────────────┴───────────────────────┴───────────────────────┘
)"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_greaterOrEquals = {1, 1};
    FunctionDocumentation::Category category_greaterOrEquals = FunctionDocumentation::Category::Comparison;
    FunctionDocumentation documentation_greaterOrEquals = {
        description_greaterOrEquals,
        syntax_greaterOrEquals,
        arguments_greaterOrEquals,
        returned_value_greaterOrEquals,
        examples_greaterOrEquals,
        introduced_in_greaterOrEquals,
        category_greaterOrEquals
    };
    factory.registerFunction<FunctionGreaterOrEquals>(documentation_greaterOrEquals);
}

template <>
ColumnPtr FunctionComparison<GreaterOrEqualsOp, NameGreaterOrEquals>::executeTupleImpl(
    const ColumnsWithTypeAndName & x, const ColumnsWithTypeAndName & y, size_t tuple_size, size_t input_rows_count) const
{

    FunctionOverloadResolverPtr greater
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionGreater>(params));

    FunctionOverloadResolverPtr greater_or_equals
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionGreaterOrEquals>(params));

    FunctionOverloadResolverPtr func_builder_or
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionOr>());

    FunctionOverloadResolverPtr func_builder_and
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());

    FunctionOverloadResolverPtr func_builder_equals
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionEquals>(params));

    return executeTupleLessGreaterImpl(
        greater,
        greater_or_equals,
        func_builder_and,
        func_builder_or,
        func_builder_equals,
        x, y, tuple_size, input_rows_count);
}

}
