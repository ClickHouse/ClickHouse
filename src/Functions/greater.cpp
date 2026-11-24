#include <Functions/FunctionFactory.h>
#include <Functions/FunctionResultSetting.h>
#include <Core/Settings.h>
#include <Functions/FunctionsComparison.h>
#include <Functions/FunctionsLogical.h>

namespace DB
{

using FunctionGreater = FunctionComparison<GreaterOp, NameGreater>;
using FunctionEquals = FunctionComparison<EqualsOp, NameEquals>;

/// Instantiated in equals.cpp.
extern template class FunctionComparison<EqualsOp, NameEquals>;

namespace Setting
{
extern const SettingsBool decimal_check_overflow;
extern const SettingsBool validate_enum_literals_in_operators;
extern const SettingsBool use_variant_default_implementation_for_comparisons;
extern const SettingsDateTimeInputFormat cast_string_to_date_time_mode;
extern const SettingsDateTimeOverflowBehavior date_time_overflow_behavior;
}

constexpr FunctionResultSetting result_settings[] = {Setting::decimal_check_overflow, Setting::validate_enum_literals_in_operators, Setting::use_variant_default_implementation_for_comparisons, Setting::cast_string_to_date_time_mode, Setting::date_time_overflow_behavior};

REGISTER_FUNCTION(Greater)
{
    // Documentation for greater
    FunctionDocumentation::Description description = "Compares two values for greater-than relation.";
    FunctionDocumentation::Syntax syntax = R"(
    greater(a, b)
    -- a > b
)";
    FunctionDocumentation::Arguments arguments = {
        {"a", "First value.<sup>[*](#comparison-rules)</sup>"},
        {"b", "Second value.<sup>[*](#comparison-rules)</sup>"}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if `a` is greater than `b`, otherwise `0`", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", "SELECT 2 > 1, 1 > 2;", R"(
┌─greater(2, 1)─┬─greater(1, 2)─┐
│             1 │             0 │
└───────────────┴───────────────┘
)"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Comparison;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};
    factory.registerFunction(FunctionGreater::name, &FunctionGreater::create, documentation, FunctionFactory::Case::Sensitive, result_settings);
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

template <>
ColumnPtr FunctionComparison<GreaterOp, NameGreater>::executeArrayLexicographic(
    const ColumnWithTypeAndName & column_type_name0,
    const ColumnWithTypeAndName & column_type_name1,
    size_t input_rows_count) const
{
    FunctionOverloadResolverPtr equals_resolver
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionEquals>(params));
    FunctionOverloadResolverPtr order_resolver
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionGreater>(params));

    return executeArrayLexicographicLessGreaterImpl(
        equals_resolver,
        order_resolver,
        column_type_name0,
        column_type_name1,
        input_rows_count);
}

/// Explicit instantiation definition, see the comment in equals.cpp. Must come after the member specializations above.
template class FunctionComparison<GreaterOp, NameGreater>;

}
