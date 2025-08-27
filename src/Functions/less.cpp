#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsComparison.h>
#include <Functions/FunctionsLogical.h>


namespace DB
{

using FunctionLess = FunctionComparison<LessOp, NameLess>;
using FunctionEquals = FunctionComparison<EqualsOp, NameEquals>;
extern template class FunctionComparison<EqualsOp, NameEquals>;

REGISTER_FUNCTION(Less)
{
    // Documentation for less
    FunctionDocumentation::Description description_less = "Compares two values for less-than relation.";
    FunctionDocumentation::Syntax syntax_less = R"(
    less(a, b)
    -- a < b
)";
    FunctionDocumentation::Arguments arguments_less = {
        {"a", "First value.<sup>[*](#comparison-rules)</sup>"},
        {"b", "Second value.<sup>[*](#comparison-rules)</sup>"}
    };
    FunctionDocumentation::ReturnedValue returned_value_less = "Returns `1` if `a` is less than `b`, otherwise `0`. [`UInt8`](/sql-reference/data-types/int-uint/)";
    FunctionDocumentation::Examples examples_less = {
        {"Usage example", "SELECT 1 < 2, 2 < 1;", R"(
┌─less(1, 2)─┬─less(2, 1)─┐
│          1 │          0 │
└────────────┴────────────┘
)"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_less = {1, 1};
    FunctionDocumentation::Category category_less = FunctionDocumentation::Category::Comparison;
    FunctionDocumentation documentation_less = {
        description_less,
        syntax_less,
        arguments_less,
        returned_value_less,
        examples_less,
        introduced_in_less,
        category_less
    };
    factory.registerFunction<FunctionLess>(documentation_less);
}

template <>
ColumnPtr FunctionComparison<LessOp, NameLess>::executeTupleImpl(
    const ColumnsWithTypeAndName & x, const ColumnsWithTypeAndName & y, size_t tuple_size, size_t input_rows_count) const
{
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
        less,
        func_builder_and,
        func_builder_or,
        func_builder_equals,
        x, y, tuple_size, input_rows_count);
}

}
