#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsComparison.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/isNotDistinctFrom.h>

namespace DB
{

using FunctionNotEquals = FunctionComparison<NotEqualsOp, NameNotEquals>;
using FunctionEquals = FunctionComparison<EqualsOp, NameEquals>;
extern template class FunctionComparison<EqualsOp, NameEquals>;

REGISTER_FUNCTION(NotEquals)
{
    // Documentation for notEquals
    FunctionDocumentation::Description description = "Compares two values for inequality.";
    FunctionDocumentation::Syntax syntax = R"(
    notEquals(a, b)
    -- a != b
    -- a <> b
)";
    FunctionDocumentation::Arguments arguments = {
        {"a", "First value.<sup>[*](#comparison-rules)</sup>"},
        {"b", "Second value.<sup>[*](#comparison-rules)</sup>"}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if `a` is not equal to `b`, otherwise `0`.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", "SELECT 1 != 2, 1 != 1;", R"(
┌─notEquals(1, 2)─┬─notEquals(1, 1)─┐
│               1 │               0 │
└─────────────────┴─────────────────┘
)"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Comparison;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};
    factory.registerFunction<FunctionNotEquals>(documentation);
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

template <>
ColumnPtr FunctionComparison<NotEqualsOp, NameNotEquals>::executeArrayLexicographic(
    const ColumnWithTypeAndName & column_type_name0,
    const ColumnWithTypeAndName & column_type_name1,
    size_t input_rows_count) const
{
    /// Array element equality is null-as-value at every nesting level, so use the null-safe equals
    /// comparator: it returns a definite UInt8
    FunctionOverloadResolverPtr equals_resolver
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionIsNotDistinctFrom>(params));

    return executeArrayLexicographicEqualityImpl(
        equals_resolver,
        column_type_name0,
        column_type_name1,
        input_rows_count);
}

}
