#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsComparison.h>
#include <Functions/FunctionsLogical.h>


namespace DB
{

using FunctionEquals = FunctionComparison<EqualsOp, NameEquals>;

REGISTER_FUNCTION(Equals)
{
    // Documentation for equals
    FunctionDocumentation::Description description_equals = "Compares two values for equality.";
    FunctionDocumentation::Syntax syntax_equals = R"(
        equals(a, b)
        -- a = b
        -- a == b
    )";
    FunctionDocumentation::Arguments arguments_equals = {
        {"a", "First value.<sup>[*](#comparison-rules)</sup>"},
        {"b", "Second value.<sup>[*](#comparison-rules)</sup>"}
    };
    FunctionDocumentation::ReturnedValue returned_value_equals = "Returns `1` if `a` is equal to `b`, otherwise `0`. [`UInt8`](/sql-reference/data-types/int-uint/)";
    FunctionDocumentation::Examples examples_equals = {
        {"Usage example", "SELECT 1 = 1, 1 = 2;", R"(
┌─equals(1, 1)─┬─equals(1, 2)─┐
│            1 │            0 │
└──────────────┴──────────────┘
)"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_equals = {1, 1};
    FunctionDocumentation::Category category_equals = FunctionDocumentation::Category::Comparison;
    FunctionDocumentation documentation_equals = {
        description_equals,
        syntax_equals,
        arguments_equals,
        returned_value_equals,
        examples_equals,
        introduced_in_equals,
        category_equals
    };
    factory.registerFunction<FunctionEquals>(documentation_equals);
}

template <>
ColumnPtr FunctionComparison<EqualsOp, NameEquals>::executeTupleImpl(
    const ColumnsWithTypeAndName & x, const ColumnsWithTypeAndName & y, size_t tuple_size, size_t input_rows_count) const
{
    FunctionOverloadResolverPtr func_builder_equals
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionEquals>(params));

    FunctionOverloadResolverPtr func_builder_and
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());

    return executeTupleEqualityImpl(
        func_builder_equals,
        func_builder_and,
        x, y, tuple_size, input_rows_count);
}

}
