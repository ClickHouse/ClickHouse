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

FunctionBasePtr buildCast(const ContextPtr & context, const DataTypePtr & source, const String & target)
{
    tryRegisterFunctions();
    auto string_type = std::make_shared<DataTypeString>();
    return FunctionFactory::instance()
        .get("CAST", context)
        ->build({
            {nullptr, source, "value"},
            {string_type->createColumnConst(1, Field(target)), string_type, "target_type"},
        });
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

TEST(BloomFilterConversionMatching, NativeNumericToStringIgnoresInputParsingSettings)
{
    auto left_context = makeContextWithSetting("input_format_parallel_parsing", "0");
    auto right_context = makeContextWithSetting("input_format_parallel_parsing", "1");
    const std::vector<String> numeric_type_names{
        "UInt8", "UInt16", "UInt32", "UInt64", "UInt128", "UInt256",
        "Int8", "Int16", "Int32", "Int64", "Int128", "Int256",
        "BFloat16", "Float32", "Float64",
    };

    for (const auto & type_name : numeric_type_names)
    {
        const auto numeric_type = getType(type_name);
        auto left = buildFunctionBase("toString", left_context, {numeric_type});
        auto right = buildFunctionBase("toString", right_context, {numeric_type});

        ASSERT_NE(getFunctionHash(*left), getFunctionHash(*right)) << type_name;
        EXPECT_TRUE(areNativeConversionsEquivalentForBloomFilterExpressionMatching(*left, *right)) << type_name;
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
    const auto datetime64 = getType("DateTime64(6)");
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

TEST(BloomFilterConversionMatching, NativeIntegerToUInt8AndUInt64IgnoreInputParsingSettings)
{
    auto left_context = makeContextWithSetting("input_format_parallel_parsing", "0");
    auto right_context = makeContextWithSetting("input_format_parallel_parsing", "1");
    const std::vector<String> conversion_names{"toUInt8", "toUInt64"};
    const std::vector<String> integer_type_names{
        "UInt8",
        "UInt16",
        "UInt32",
        "UInt64",
        "UInt128",
        "UInt256",
        "Int8",
        "Int16",
        "Int32",
        "Int64",
        "Int128",
        "Int256",
    };

    for (const auto & conversion_name : conversion_names)
    {
        for (const auto & type_name : integer_type_names)
        {
            const auto input_type = getType(type_name);
            auto left = buildFunctionBase(conversion_name, left_context, {input_type});
            auto right = buildFunctionBase(conversion_name, right_context, {input_type});

            ASSERT_NE(getFunctionHash(*left), getFunctionHash(*right)) << conversion_name << ", " << type_name;
            EXPECT_TRUE(areNativeConversionsEquivalentForBloomFilterExpressionMatching(*left, *right))
                << conversion_name << ", " << type_name;
        }
    }
}

TEST(BloomFilterConversionMatching, UInt8AndUInt64ConversionsDeclineUnauditedOrDifferentInputTypes)
{
    auto left_context = makeContextWithSetting("input_format_parallel_parsing", "0");
    auto right_context = makeContextWithSetting("input_format_parallel_parsing", "1");
    const std::vector<String> conversion_names{"toUInt8", "toUInt64"};
    const std::vector<String> unsupported_type_names{
        "String",
        "Float32",
        "Float64",
        "BFloat16",
        "Decimal64(2)",
        "Nullable(Int64)",
        "LowCardinality(Int64)",
        "Bool",
    };

    for (const auto & conversion_name : conversion_names)
    {
        for (const auto & type_name : unsupported_type_names)
        {
            const auto input_type = getType(type_name);
            auto left = buildFunctionBase(conversion_name, left_context, {input_type});
            auto right = buildFunctionBase(conversion_name, right_context, {input_type});
            EXPECT_FALSE(areNativeConversionsEquivalentForBloomFilterExpressionMatching(*left, *right))
                << conversion_name << ", " << type_name;
        }

        auto signed_input = buildFunctionBase(conversion_name, left_context, {getType("Int64")});
        auto unsigned_input = buildFunctionBase(conversion_name, right_context, {getType("UInt64")});
        EXPECT_FALSE(areNativeConversionsEquivalentForBloomFilterExpressionMatching(*signed_input, *unsigned_input)) << conversion_name;
    }

    auto signed_input = buildFunctionBase("toUInt8", left_context, {getType("Int64")});
    auto different_result = buildFunctionBase("toUInt16", right_context, {getType("Int64")});
    auto different_result_width = buildFunctionBase("toUInt64", right_context, {getType("Int64")});
    EXPECT_FALSE(areNativeConversionsEquivalentForBloomFilterExpressionMatching(*signed_input, *different_result));
    EXPECT_FALSE(areNativeConversionsEquivalentForBloomFilterExpressionMatching(*signed_input, *different_result_width));
}

TEST(BloomFilterConversionMatching, NativeCastAuthenticationIncludesWholeArrayConversions)
{
    auto context = makeContextWithSetting("cast_string_to_date_time_mode", "best_effort");
    auto scalar = buildCast(context, getType("String"), "DateTime('UTC')");
    auto array = buildCast(context, getType("Array(String)"), "Array(DateTime('UTC'))");
    auto non_cast = buildFunctionBase("toUInt8", context, {getType("UInt64")});

    EXPECT_TRUE(isNativeCastForBloomFilterExpressionMatching(*scalar));
    EXPECT_TRUE(isNativeCastForBloomFilterExpressionMatching(*array));
    EXPECT_FALSE(isNativeCastForBloomFilterExpressionMatching(*non_cast));
}

TEST(BloomFilterConversionMatching, AuthenticatesOnlyExactDateTimeModeDependentConversionFamilies)
{
    auto context = makeContextWithSetting("cast_string_to_date_time_mode", "best_effort");
    const auto string_type = getType("String");
    const auto scale_type = getType("UInt8");

    const auto to_datetime = buildFunctionBase("toDateTime", context, {string_type});
    const auto to_datetime32 = buildFunctionBase("toDateTime32", context, {string_type});
    const auto to_datetime64 = buildFunctionBase("toDateTime64", context, {string_type, scale_type}, 3);
    const auto to_unix_timestamp = buildFunctionBase("toUnixTimestamp", context, {string_type});

    EXPECT_TRUE(isNativeDateTimeModeDependentConversionForBloomFilterExpressionMatching(*to_datetime));
    EXPECT_TRUE(isNativeDateTimeModeDependentConversionForBloomFilterExpressionMatching(*to_datetime32));
    EXPECT_TRUE(isNativeDateTimeModeDependentConversionForBloomFilterExpressionMatching(*to_datetime64));
    EXPECT_TRUE(isNativeDateTimeModeDependentConversionForBloomFilterExpressionMatching(*to_unix_timestamp));

    const auto fixed_parser = buildFunctionBase("toDateTimeOrNull", context, {string_type});
    const auto unrelated_conversion = buildFunctionBase("toUInt8", context, {getType("UInt64")});
    EXPECT_FALSE(isNativeDateTimeModeDependentConversionForBloomFilterExpressionMatching(*fixed_parser));
    EXPECT_FALSE(isNativeDateTimeModeDependentConversionForBloomFilterExpressionMatching(*unrelated_conversion));
}

TEST(BloomFilterConversionMatching, AuthenticatesSettingsCapturingNativeConversionFamilies)
{
    auto context = makeContextWithSetting("precise_float_parsing", "0");
    const auto string_type = getType("String");
    const auto uint64_type = getType("UInt64");

    const auto native_convert = buildFunctionBase("toUInt8", context, {uint64_type});
    const auto native_convert_from_string = buildFunctionBase("toFloat64OrZero", context, {string_type});
    const auto best_effort_parser = buildFunctionBase("parseDateTimeBestEffort", context, {string_type});
    const auto native_cast = buildCast(context, getType("Array(String)"), "Array(Float64)");
    const auto unrelated_function = buildFunctionBase("plus", context, {uint64_type, uint64_type});

    EXPECT_TRUE(isNativeSettingsCapturingConversionForBloomFilterExpressionMatching(*native_convert));
    EXPECT_TRUE(isNativeSettingsCapturingConversionForBloomFilterExpressionMatching(*native_convert_from_string));
    EXPECT_TRUE(isNativeSettingsCapturingConversionForBloomFilterExpressionMatching(*best_effort_parser));
    EXPECT_TRUE(isNativeSettingsCapturingConversionForBloomFilterExpressionMatching(*native_cast));
    EXPECT_FALSE(isNativeSettingsCapturingConversionForBloomFilterExpressionMatching(*unrelated_function));
}
