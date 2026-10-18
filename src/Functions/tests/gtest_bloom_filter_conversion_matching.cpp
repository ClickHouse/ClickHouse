#include <gtest/gtest.h>

#include <Common/SipHash.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <Columns/ColumnConst.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionConversionMatching.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>

#include <optional>
#include <vector>

using namespace DB;

namespace
{

ContextMutablePtr makeContextWithSetting(const String & name, const String & value)
{
    auto context = Context::createCopy(getContext().context);
    context->setSetting(name, value);
    return context;
}

FunctionBasePtr buildFunctionBase(
    const String & function_name,
    const ContextPtr & context,
    const DataTypes & argument_types,
    std::optional<UInt64> decimal_scale = {})
{
    tryRegisterFunctions();
    auto resolver = FunctionFactory::instance().get(function_name, context);

    ColumnsWithTypeAndName arguments;
    arguments.reserve(argument_types.size());
    for (size_t i = 0; i < argument_types.size(); ++i)
    {
        ColumnPtr column;
        if (decimal_scale && i == 1)
            column = argument_types[i]->createColumnConst(1, Field(*decimal_scale));
        arguments.emplace_back(std::move(column), argument_types[i], "");
    }

    return resolver->build(arguments);
}

UInt64 getFunctionHash(const IFunctionBase & function)
{
    SipHash hash;
    function.updateHash(hash);
    return hash.get64();
}

DataTypePtr getType(const String & name)
{
    return DataTypeFactory::instance().get(name);
}

}

TEST(BloomFilterConversionMatching, NativeNumericToDecimalIgnoresInputParsingSettings)
{
    auto left_context = makeContextWithSetting("input_format_parallel_parsing", "0");
    auto right_context = makeContextWithSetting("input_format_parallel_parsing", "1");
    const DataTypes argument_types{std::make_shared<DataTypeUInt64>(), std::make_shared<DataTypeUInt64>()};

    for (const auto & function_name : {"toDecimal32", "toDecimal64", "toDecimal128", "toDecimal256"})
    {
        auto left = buildFunctionBase(function_name, left_context, argument_types, 2);
        auto right = buildFunctionBase(function_name, right_context, argument_types, 2);

        ASSERT_NE(getFunctionHash(*left), getFunctionHash(*right)) << function_name;
        EXPECT_TRUE(areNativeConversionsEquivalentForBloomFilterExpressionMatching(*left, *right)) << function_name;
    }
}

TEST(BloomFilterConversionMatching, DecimalToStringIgnoresInputParsingSettings)
{
    auto left_context = makeContextWithSetting("input_format_parallel_parsing", "0");
    auto right_context = makeContextWithSetting("input_format_parallel_parsing", "1");
    const std::vector<DataTypePtr> decimal_types{
        std::make_shared<DataTypeDecimal32>(9, 2),
        std::make_shared<DataTypeDecimal64>(18, 2),
        std::make_shared<DataTypeDecimal128>(38, 2),
        std::make_shared<DataTypeDecimal256>(76, 2),
    };

    for (const auto & decimal_type : decimal_types)
    {
        auto left = buildFunctionBase("toString", left_context, {decimal_type});
        auto right = buildFunctionBase("toString", right_context, {decimal_type});

        ASSERT_NE(getFunctionHash(*left), getFunctionHash(*right));
        EXPECT_TRUE(areNativeConversionsEquivalentForBloomFilterExpressionMatching(*left, *right));
    }
}

TEST(BloomFilterConversionMatching, DecimalPrecisionAndScaleMustMatch)
{
    auto context = makeContextWithSetting("input_format_parallel_parsing", "0");
    const DataTypePtr decimal_12_2 = std::make_shared<DataTypeDecimal64>(12, 2);
    const DataTypePtr decimal_18_2 = std::make_shared<DataTypeDecimal64>(18, 2);
    const DataTypePtr decimal_18_3 = std::make_shared<DataTypeDecimal64>(18, 3);

    auto precision_left = buildFunctionBase("toString", context, {decimal_12_2});
    auto precision_right = buildFunctionBase("toString", context, {decimal_18_2});
    EXPECT_FALSE(areNativeConversionsEquivalentForBloomFilterExpressionMatching(*precision_left, *precision_right));

    auto scale_left = buildFunctionBase("toString", context, {decimal_18_2});
    auto scale_right = buildFunctionBase("toString", context, {decimal_18_3});
    EXPECT_FALSE(areNativeConversionsEquivalentForBloomFilterExpressionMatching(*scale_left, *scale_right));

    /// Here the helper declines because the result Decimal types have different scales. The outer
    /// expression matcher must still compare the toDecimal scale constants itself.
    const DataTypes numeric_arguments{std::make_shared<DataTypeUInt64>(), std::make_shared<DataTypeUInt64>()};
    auto decimal_scale_2 = buildFunctionBase("toDecimal64", context, numeric_arguments, 2);
    auto decimal_scale_3 = buildFunctionBase("toDecimal64", context, numeric_arguments, 3);
    EXPECT_FALSE(areNativeConversionsEquivalentForBloomFilterExpressionMatching(*decimal_scale_2, *decimal_scale_3));
}

TEST(BloomFilterConversionMatching, UnsupportedStringAndCustomSerializationPathsRemainStrict)
{
    auto left_context = makeContextWithSetting("precise_float_parsing", "0");
    auto right_context = makeContextWithSetting("precise_float_parsing", "1");
    const DataTypes decimal_arguments{std::make_shared<DataTypeString>(), std::make_shared<DataTypeUInt64>()};
    auto left_decimal = buildFunctionBase("toDecimal64", left_context, decimal_arguments, 2);
    auto right_decimal = buildFunctionBase("toDecimal64", right_context, decimal_arguments, 2);

    ASSERT_NE(getFunctionHash(*left_decimal), getFunctionHash(*right_decimal));
    EXPECT_FALSE(areNativeConversionsEquivalentForBloomFilterExpressionMatching(*left_decimal, *right_decimal));

    auto left_format_context = makeContextWithSetting(
        "date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands", "0");
    auto right_format_context = makeContextWithSetting(
        "date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands", "1");
    const auto datetime64 = getType("DateTime64(3)");
    auto left_datetime_string = buildFunctionBase("toString", left_format_context, {datetime64});
    auto right_datetime_string = buildFunctionBase("toString", right_format_context, {datetime64});

    ASSERT_NE(getFunctionHash(*left_datetime_string), getFunctionHash(*right_datetime_string));
    EXPECT_FALSE(areNativeConversionsEquivalentForBloomFilterExpressionMatching(*left_datetime_string, *right_datetime_string));

    const auto ipv4 = getType("IPv4");
    auto left_custom_string = buildFunctionBase("toString", left_context, {ipv4});
    auto right_custom_string = buildFunctionBase("toString", right_context, {ipv4});
    EXPECT_FALSE(areNativeConversionsEquivalentForBloomFilterExpressionMatching(*left_custom_string, *right_custom_string));

    const DataTypePtr nullable_decimal = makeNullable(std::make_shared<DataTypeDecimal64>(18, 2));
    auto left_wrapped_string = buildFunctionBase("toString", left_context, {nullable_decimal});
    auto right_wrapped_string = buildFunctionBase("toString", right_context, {nullable_decimal});
    EXPECT_FALSE(areNativeConversionsEquivalentForBloomFilterExpressionMatching(*left_wrapped_string, *right_wrapped_string));
}
