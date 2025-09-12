#include <Functions/FunctionFactory.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Core/DecimalFunctions.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>


/// ------------------------------------------------------------------------------------------------------------------------------
/// The functions in this file are deprecated and should be removed in favor of functions 'snowflakeIDToDateTime[64]' and
/// 'dateTime[64]ToSnowflakeID' by summer 2025. Please also mark setting `allow_deprecated_snowflake_conversion_functions` as obsolete then.
/// ------------------------------------------------------------------------------------------------------------------------------

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_deprecated_snowflake_conversion_functions;
    extern const SettingsBool allow_nonconst_timezone_arguments;
}

namespace ErrorCodes
{
    extern const int DEPRECATED_FUNCTION;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/** According to Twitter's post on Snowflake, we can extract the timestamp for a snowflake ID by right shifting
 * the snowflake ID by 22 bits(10 bits machine ID and 12 bits sequence ID) and adding the Twitter epoch time of 1288834974657.
 * https://en.wikipedia.org/wiki/Snowflake_ID
 * https://blog.twitter.com/engineering/en_us/a/2010/announcing-snowflake
 * https://ws-dl.blogspot.com/2019/08/2019-08-03-tweetedat-finding-tweet.html
*/
constexpr size_t snowflake_epoch = 1288834974657L;
constexpr int time_shift = 22;

class FunctionDateTimeToSnowflake : public IFunction
{
private:
    const bool allow_deprecated_snowflake_conversion_functions;

public:
    static constexpr auto name = "dateTimeToSnowflake";

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionDateTimeToSnowflake>(context);
    }

    explicit FunctionDateTimeToSnowflake(ContextPtr context)
        : allow_deprecated_snowflake_conversion_functions(context->getSettingsRef()[Setting::allow_deprecated_snowflake_conversion_functions])
    {}

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"value", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isDateTime), nullptr, "DateTime"}
        };
        validateFunctionArguments(*this, arguments, args);

        return std::make_shared<DataTypeInt64>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (!allow_deprecated_snowflake_conversion_functions)
            throw Exception(ErrorCodes::DEPRECATED_FUNCTION, "Function {} is deprecated, to enable it set setting 'allow_deprecated_snowflake_conversion_functions' to 'true'", getName());

        const auto & src = arguments[0];
        const auto & src_column = *src.column;

        auto res_column = ColumnInt64::create(input_rows_count);
        auto & res_data = res_column->getData();

        const auto & src_data = typeid_cast<const ColumnUInt32 &>(src_column).getData();
        for (size_t i = 0; i < input_rows_count; ++i)
            res_data[i] = (Int64(src_data[i]) * 1000 - snowflake_epoch) << time_shift;

        return res_column;
    }
};

class FunctionSnowflakeToDateTime : public IFunction
{
private:
    const bool allow_nonconst_timezone_arguments;
    const bool allow_deprecated_snowflake_conversion_functions;

public:
    static constexpr auto name = "snowflakeToDateTime";

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionSnowflakeToDateTime>(context);
    }

    explicit FunctionSnowflakeToDateTime(ContextPtr context)
        : allow_nonconst_timezone_arguments(context->getSettingsRef()[Setting::allow_nonconst_timezone_arguments])
        , allow_deprecated_snowflake_conversion_functions(context->getSettingsRef()[Setting::allow_deprecated_snowflake_conversion_functions])
    {}

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"value", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isInt64), nullptr, "Int64"}
        };
        FunctionArgumentDescriptors optional_args{
            {"time_zone", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"}
        };
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        String timezone;
        if (arguments.size() == 2)
            timezone = extractTimeZoneNameFromFunctionArguments(arguments, 1, 0, allow_nonconst_timezone_arguments);

        return std::make_shared<DataTypeDateTime>(timezone);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (!allow_deprecated_snowflake_conversion_functions)
            throw Exception(ErrorCodes::DEPRECATED_FUNCTION, "Function {} is deprecated, to enable it set setting 'allow_deprecated_snowflake_conversion_functions' to 'true'", getName());

        const auto & src = arguments[0];
        const auto & src_column = *src.column;

        auto res_column = ColumnUInt32::create(input_rows_count);
        auto & res_data = res_column->getData();

        if (const auto * src_column_non_const = typeid_cast<const ColumnInt64 *>(&src_column))
        {
            const auto & src_data = src_column_non_const->getData();
            for (size_t i = 0; i < input_rows_count; ++i)
                res_data[i] = static_cast<UInt32>(
                    ((src_data[i] >> time_shift) + snowflake_epoch) / 1000);
        }
        else if (const auto * src_column_const = typeid_cast<const ColumnConst *>(&src_column))
        {
            Int64 src_val = src_column_const->getValue<Int64>();
            for (size_t i = 0; i < input_rows_count; ++i)
                res_data[i] = static_cast<UInt32>(
                    ((src_val >> time_shift) + snowflake_epoch) / 1000);
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal argument for function {}", name);

        return res_column;
    }
};


class FunctionDateTime64ToSnowflake : public IFunction
{
private:
    const bool allow_deprecated_snowflake_conversion_functions;

public:
    static constexpr auto name = "dateTime64ToSnowflake";

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionDateTime64ToSnowflake>(context);
    }

    explicit FunctionDateTime64ToSnowflake(ContextPtr context)
        : allow_deprecated_snowflake_conversion_functions(context->getSettingsRef()[Setting::allow_deprecated_snowflake_conversion_functions])
    {}

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"value", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isDateTime64), nullptr, "DateTime64"}
        };
        validateFunctionArguments(*this, arguments, args);

        return std::make_shared<DataTypeInt64>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (!allow_deprecated_snowflake_conversion_functions)
            throw Exception(ErrorCodes::DEPRECATED_FUNCTION, "Function {} is deprecated, to enable it set setting 'allow_deprecated_snowflake_conversion_functions' to true", getName());

        const auto & src = arguments[0];

        const auto & src_column = *src.column;
        auto res_column = ColumnInt64::create(input_rows_count);
        auto & res_data = res_column->getData();

        const auto & src_data = typeid_cast<const ColumnDecimal<DateTime64> &>(src_column).getData();

        /// timestamps in snowflake-ids are millisecond-based, convert input to milliseconds
        UInt32 src_scale = getDecimalScale(*arguments[0].type);
        Int64 multiplier_msec = DecimalUtils::scaleMultiplier<DateTime64>(3);
        Int64 multiplier_src = DecimalUtils::scaleMultiplier<DateTime64>(src_scale);
        auto factor = multiplier_msec / static_cast<double>(multiplier_src);

        for (size_t i = 0; i < input_rows_count; ++i)
            res_data[i] = static_cast<Int64>(src_data[i] * factor - snowflake_epoch) << time_shift;

        return res_column;
    }
};


class FunctionSnowflakeToDateTime64 : public IFunction
{
private:
    const bool allow_nonconst_timezone_arguments;
    const bool allow_deprecated_snowflake_conversion_functions;

public:
    static constexpr auto name = "snowflakeToDateTime64";

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionSnowflakeToDateTime64>(context);
    }

    explicit FunctionSnowflakeToDateTime64(ContextPtr context)
        : allow_nonconst_timezone_arguments(context->getSettingsRef()[Setting::allow_nonconst_timezone_arguments])
        , allow_deprecated_snowflake_conversion_functions(context->getSettingsRef()[Setting::allow_deprecated_snowflake_conversion_functions])
    {}

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"value", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isInt64), nullptr, "Int64"}
        };
        FunctionArgumentDescriptors optional_args{
            {"time_zone", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"}
        };
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        String timezone;
        if (arguments.size() == 2)
            timezone = extractTimeZoneNameFromFunctionArguments(arguments, 1, 0, allow_nonconst_timezone_arguments);

        return std::make_shared<DataTypeDateTime64>(3, timezone);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (!allow_deprecated_snowflake_conversion_functions)
            throw Exception(ErrorCodes::DEPRECATED_FUNCTION, "Function {} is deprecated, to enable it set setting 'allow_deprecated_snowflake_conversion_functions' to true", getName());

        const auto & src = arguments[0];
        const auto & src_column = *src.column;

        auto res_column = ColumnDecimal<DateTime64>::create(input_rows_count, 3);
        auto & res_data = res_column->getData();

        if (const auto * src_column_non_const = typeid_cast<const ColumnInt64 *>(&src_column))
        {
            const auto & src_data = src_column_non_const->getData();
            for (size_t i = 0; i < input_rows_count; ++i)
                res_data[i] = (src_data[i] >> time_shift) + snowflake_epoch;
        }
        else if (const auto * src_column_const = typeid_cast<const ColumnConst *>(&src_column))
        {
            Int64 src_val = src_column_const->getValue<Int64>();
            for (size_t i = 0; i < input_rows_count; ++i)
                res_data[i] = (src_val >> time_shift) + snowflake_epoch;
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal argument for function {}", name);

        return res_column;
    }
};

}

REGISTER_FUNCTION(LegacySnowflakeConversion)
{
    /// snowflakeToDateTime documentation
    FunctionDocumentation::Description description_snowflakeToDateTime = R"(
<DeprecatedBadge/>

:::warning
This function is deprecated and can only be used if setting [`allow_deprecated_snowflake_conversion_functions`](../../operations/settings/settings.md#allow_deprecated_snowflake_conversion_functions) is enabled.
The function will be removed at some point in future.

Please use function [`snowflakeIDToDateTime`](#snowflakeIDToDateTime) instead.
:::

Extracts the timestamp component of a [Snowflake ID](https://en.wikipedia.org/wiki/Snowflake_ID) in [DateTime](../data-types/datetime.md) format.
    )";
    FunctionDocumentation::Syntax syntax_snowflakeToDateTime = "snowflakeToDateTime(value[, time_zone])";
    FunctionDocumentation::Arguments arguments_snowflakeToDateTime = {
        {"value", "Snowflake ID.", {"Int64"}},
        {"time_zone", "Optional. [Timezone](/operations/server-configuration-parameters/settings.md#timezone). The function parses `time_string` according to the timezone.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_snowflakeToDateTime = {"Returns the timestamp component of `value`.", {"DateTime"}};
    FunctionDocumentation::Examples examples_snowflakeToDateTime = {
    {
        "Usage example",
        R"(
SELECT snowflakeToDateTime(CAST('1426860702823350272', 'Int64'), 'UTC');
        )",
        R"(
┌─snowflakeToDateTime(CAST('1426860702823350272', 'Int64'), 'UTC')─┐
│                                              2021-08-15 10:57:56 │
└──────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_snowflakeToDateTime = {21, 10};
    FunctionDocumentation::Category category_snowflakeToDateTime = FunctionDocumentation::Category::UUID;
    FunctionDocumentation documentation_snowflakeToDateTime = {description_snowflakeToDateTime, syntax_snowflakeToDateTime, arguments_snowflakeToDateTime, returned_value_snowflakeToDateTime, examples_snowflakeToDateTime, introduced_in_snowflakeToDateTime, category_snowflakeToDateTime};

    factory.registerFunction<FunctionSnowflakeToDateTime>(documentation_snowflakeToDateTime);

    /// snowflakeToDateTime64 documentation
    FunctionDocumentation::Description description_snowflakeToDateTime64 = R"(
<DeprecatedBadge/>

:::warning
This function is deprecated and can only be used if setting [`allow_deprecated_snowflake_conversion_functions`](../../operations/settings/settings.md#allow_deprecated_snowflake_conversion_functions) is enabled.
The function will be removed at some point in future.

Please use function [`snowflakeIDToDateTime64`](#snowflakeIDToDateTime64) instead.
:::

Extracts the timestamp component of a [Snowflake ID](https://en.wikipedia.org/wiki/Snowflake_ID) in [DateTime64](../data-types/datetime64.md) format.

    )";
    FunctionDocumentation::Syntax syntax_snowflakeToDateTime64 = "snowflakeToDateTime64(value[, time_zone])";
    FunctionDocumentation::Arguments arguments_snowflakeToDateTime64 = {
        {"value", "Snowflake ID.", {"Int64"}},
        {"time_zone", "Optional. [Timezone](/operations/server-configuration-parameters/settings.md#timezone). The function parses `time_string` according to the timezone.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_snowflakeToDateTime64 = {"Returns the timestamp component of `value`.", {"DateTime64(3)"}};
    FunctionDocumentation::Examples examples_snowflakeToDateTime64 = {
    {
        "Usage example",
        R"(
SELECT snowflakeToDateTime64(CAST('1426860802823350272', 'Int64'), 'UTC');
        )",
        R"(
┌─snowflakeToDateTime64(CAST('1426860802823350272', 'Int64'), 'UTC')─┐
│                                            2021-08-15 10:58:19.841 │
└────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_snowflakeToDateTime64 = {21, 10};
    FunctionDocumentation::Category category_snowflakeToDateTime64 = FunctionDocumentation::Category::UUID;
    FunctionDocumentation documentation_snowflakeToDateTime64 = {description_snowflakeToDateTime64, syntax_snowflakeToDateTime64, arguments_snowflakeToDateTime64, returned_value_snowflakeToDateTime64, examples_snowflakeToDateTime64, introduced_in_snowflakeToDateTime64, category_snowflakeToDateTime64};

    factory.registerFunction<FunctionSnowflakeToDateTime64>(documentation_snowflakeToDateTime64);

    /// dateTimeToSnowflake documentation
    FunctionDocumentation::Description description_dateTimeToSnowflake = R"(

<DeprecatedBadge/>

:::warning
This function is deprecated and can only be used if setting [`allow_deprecated_snowflake_conversion_functions`](../../operations/settings/settings.md#allow_deprecated_snowflake_conversion_functions) is enabled.
The function will be removed at some point in future.

Please use function [dateTimeToSnowflakeID](#dateTimeToSnowflakeID) instead.
:::

Converts a [DateTime](../data-types/datetime.md) value to the first [Snowflake ID](https://en.wikipedia.org/wiki/Snowflake_ID) at the giving time.
    )";
    FunctionDocumentation::Syntax syntax_dateTimeToSnowflake = "dateTimeToSnowflake(value)";
    FunctionDocumentation::Arguments arguments_dateTimeToSnowflake = {
        {"value", "Date with time.", {"DateTime"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_dateTimeToSnowflake = {"Returns the input value as the first Snowflake ID at that time.", {"Int64"}};
    FunctionDocumentation::Examples examples_dateTimeToSnowflake = {
    {
        "Usage example",
        R"(
WITH toDateTime('2021-08-15 18:57:56', 'Asia/Shanghai') AS dt SELECT dateTimeToSnowflake(dt);
        )",
        R"(
┌─dateTimeToSnowflake(dt)─┐
│     1426860702823350272 │
└─────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_dateTimeToSnowflake = {21, 10};
    FunctionDocumentation::Category category_dateTimeToSnowflake = FunctionDocumentation::Category::UUID;
    FunctionDocumentation documentation_dateTimeToSnowflake = {description_dateTimeToSnowflake, syntax_dateTimeToSnowflake, arguments_dateTimeToSnowflake, returned_value_dateTimeToSnowflake, examples_dateTimeToSnowflake, introduced_in_dateTimeToSnowflake, category_dateTimeToSnowflake};

    factory.registerFunction<FunctionDateTimeToSnowflake>(documentation_dateTimeToSnowflake);

    /// dateTime64ToSnowflake documentation
    FunctionDocumentation::Description description_dateTime64ToSnowflake = R"(
<DeprecatedBadge/>

:::warning
This function is deprecated and can only be used if setting [`allow_deprecated_snowflake_conversion_functions`](../../operations/settings/settings.md#allow_deprecated_snowflake_conversion_functions) is enabled.
The function will be removed at some point in future.

Please use function [dateTime64ToSnowflakeID](#dateTime64ToSnowflakeID) instead.
:::

Converts a [DateTime64](../data-types/datetime64.md) to the first [Snowflake ID](https://en.wikipedia.org/wiki/Snowflake_ID) at the giving time.
    )";
    FunctionDocumentation::Syntax syntax_dateTime64ToSnowflake = "dateTime64ToSnowflake(value)";
    FunctionDocumentation::Arguments arguments_dateTime64ToSnowflake = {
        {"value", "Date with time.", {"DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_dateTime64ToSnowflake = {"Returns the input value converted as the first Snowflake ID at that time.", {"Int64"}};
    FunctionDocumentation::Examples examples_dateTime64ToSnowflake = {
    {
        "Usage example",
        R"(
WITH toDateTime64('2021-08-15 18:57:56.492', 3, 'Asia/Shanghai') AS dt64 SELECT dateTime64ToSnowflake(dt64);
        )",
        R"(
┌─dateTime64ToSnowflake(dt64)─┐
│         1426860704886947840 │
└─────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_dateTime64ToSnowflake = {21, 10};
    FunctionDocumentation::Category category_dateTime64ToSnowflake = FunctionDocumentation::Category::UUID;
    FunctionDocumentation documentation_dateTime64ToSnowflake = {description_dateTime64ToSnowflake, syntax_dateTime64ToSnowflake, arguments_dateTime64ToSnowflake, returned_value_dateTime64ToSnowflake, examples_dateTime64ToSnowflake, introduced_in_dateTime64ToSnowflake, category_dateTime64ToSnowflake};

    factory.registerFunction<FunctionDateTime64ToSnowflake>(documentation_dateTime64ToSnowflake);
}

}
