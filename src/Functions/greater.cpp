#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsComparison.h>
#include <Functions/FunctionsLogical.h>

namespace DB
{

using FunctionGreater = FunctionComparison<GreaterOp, NameGreater>;
using FunctionEquals = FunctionComparison<EqualsOp, NameEquals>;
extern template class FunctionComparison<EqualsOp, NameEquals>;

REGISTER_FUNCTION(Greater)
{
    // Documentation for greater
    FunctionDocumentation::Description description_greater = "Compares two values for greater-than relation.";
    FunctionDocumentation::Syntax syntax_greater = R"(
    greater(a, b)
    -- a > b
)";
    FunctionDocumentation::Arguments arguments_greater = {
        {"a", "First value.<sup>[*](#comparison-rules)</sup>"},
        {"b", "Second value.<sup>[*](#comparison-rules)</sup>"}
    };
    FunctionDocumentation::ReturnedValue returned_value_greater = "Returns `1` if `a` is greater than `b`, otherwise `0`. [`UInt8`](/sql-reference/data-types/int-uint/)";
    FunctionDocumentation::Examples examples_greater = {
        {"Usage example", "SELECT 2 > 1, 1 > 2;", R"(
┌─greater(2, 1)─┬─greater(1, 2)─┐
│             1 │             0 │
└───────────────┴───────────────┘
)"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_greater = {1, 1};
    FunctionDocumentation::Category category_greater = FunctionDocumentation::Category::Comparison;
    FunctionDocumentation documentation_greater = {
        description_greater,
        syntax_greater,
        arguments_greater,
        returned_value_greater,
        examples_greater,
        introduced_in_greater,
        category_greater
    };
    factory.registerFunction<FunctionGreater>(documentation_greater);
}

template <>
ColumnPtr FunctionComparison<GreaterOp, NameGreater>::executeTupleImpl(
    const ColumnsWithTypeAndName & x, const ColumnsWithTypeAndName & y, size_t tuple_size, size_t input_rows_count) const
{
    FunctionOverloadResolverPtr greater
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionGreater>(params));

    FunctionOverloadResolverPtr func_builder_or
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionOr>());

    FunctionOverloadResolverPtr func_builder_and
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());

    FunctionOverloadResolverPtr func_builder_equals
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionEquals>(params));

    return executeTupleLessGreaterImpl(
        greater,
        greater,
        func_builder_and,
        func_builder_or,
        func_builder_equals,
        x, y, tuple_size, input_rows_count);
}

}
