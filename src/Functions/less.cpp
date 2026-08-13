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
    FunctionDocumentation::Description description = "Compares two values for less-than relation.";
    FunctionDocumentation::Syntax syntax = R"(
    less(a, b)
    -- a < b
)";
    FunctionDocumentation::Arguments arguments = {
        {"a", "First value.<sup>[*](#comparison-rules)</sup>"},
        {"b", "Second value.<sup>[*](#comparison-rules)</sup>"}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if `a` is less than `b`, otherwise `0`", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", "SELECT 1 < 2, 2 < 1;", R"(
┌─less(1, 2)─┬─less(2, 1)─┐
│          1 │          0 │
└────────────┴────────────┘
)"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Comparison;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
    factory.registerFunction<FunctionLess>(documentation);
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
