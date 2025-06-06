#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsComparison.h>
#include <Functions/FunctionsLogical.h>


namespace DB
{

using FunctionLessOrEquals = FunctionComparison<LessOrEqualsOp, NameLessOrEquals>;
using FunctionLess = FunctionComparison<LessOp, NameLess>;
extern template class FunctionComparison<LessOp, NameLess>;
using FunctionEquals = FunctionComparison<EqualsOp, NameEquals>;
extern template class FunctionComparison<EqualsOp, NameEquals>;

REGISTER_FUNCTION(LessOrEquals)
{
    // Documentation for lessOrEquals
    FunctionDocumentation::Description description_lessOrEquals = "Compares two values for less-than-or-equal-to relation.";
    FunctionDocumentation::Syntax syntax_lessOrEquals = R"(
    lessOrEquals(a, b)
    -- a <= b
)";
    FunctionDocumentation::Arguments arguments_lessOrEquals = {
        {"a", "First value.<sup>[*](#comparison-rules)</sup>"},
        {"b", "Second value.<sup>[*](#comparison-rules)</sup>"}
    };
    FunctionDocumentation::ReturnedValue returned_value_lessOrEquals = "Returns `1` if `a` is less than or equal to `b`, otherwise `0`. [`UInt8`](/sql-reference/data-types/int-uint/)";
    FunctionDocumentation::Examples examples_lessOrEquals = {
        {"Usage example", "SELECT 1 <= 2, 2 <= 2, 3 <= 2;", R"(
┌─lessOrEquals(1, 2)─┬─lessOrEquals(2, 2)─┬─lessOrEquals(3, 2)─┐
│                  1 │                  1 │                  0 │
└────────────────────┴────────────────────┴────────────────────┘
)"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_lessOrEquals = {1, 1};
    FunctionDocumentation::Category category_lessOrEquals = FunctionDocumentation::Category::Comparison;
    FunctionDocumentation documentation_lessOrEquals = {
        description_lessOrEquals,
        syntax_lessOrEquals,
        arguments_lessOrEquals,
        returned_value_lessOrEquals,
        examples_lessOrEquals,
        introduced_in_lessOrEquals,
        category_lessOrEquals
    };
    factory.registerFunction<FunctionLessOrEquals>(documentation_lessOrEquals);
}

template <>
ColumnPtr FunctionComparison<LessOrEqualsOp, NameLessOrEquals>::executeTupleImpl(
    const ColumnsWithTypeAndName & x, const ColumnsWithTypeAndName & y, size_t tuple_size, size_t input_rows_count) const
{
    FunctionOverloadResolverPtr less_or_equals
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionLessOrEquals>(params));

    FunctionOverloadResolverPtr less
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionLess>(params));

    FunctionOverloadResolverPtr func_builder_or
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionOr>());

    FunctionOverloadResolverPtr func_builder_and
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());

    FunctionOverloadResolverPtr func_builder_equals
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionEquals>(params));

    return executeTupleLessGreaterImpl(
        less,
        less_or_equals,
        func_builder_and,
        func_builder_or,
        func_builder_equals,
        x, y, tuple_size, input_rows_count);
}

}
