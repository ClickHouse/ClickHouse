#include <Functions/FunctionFactory.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <Core/DecimalFunctions.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_nonconst_timezone_arguments;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/// See generateSnowflakeID.cpp
constexpr size_t time_shift = 22;

}

class FunctionSnowflakeIDToDateTime : public IFunction
{
private:
    const bool allow_nonconst_timezone_arguments;

public:
    static constexpr auto name = "snowflakeIDToDateTime";

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionSnowflakeIDToDateTime>(context); }
    explicit FunctionSnowflakeIDToDateTime(ContextPtr context)
        : allow_nonconst_timezone_arguments(context->getSettingsRef()[Setting::allow_nonconst_timezone_arguments])
    {}

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"value", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isUInt64), nullptr, "UInt64"}
        };
        FunctionArgumentDescriptors optional_args{
            {"epoch", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNativeUInt), isColumnConst, "const UInt*"},
            {"time_zone", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"}
        };
        validateFunctionArguments(*this, arguments, args, optional_args);

        String timezone;
        if (arguments.size() == 3)
            timezone = extractTimeZoneNameFromFunctionArguments(arguments, 2, 0, allow_nonconst_timezone_arguments);

        return std::make_shared<DataTypeDateTime>(timezone);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & col_src = *arguments[0].column;

        UInt64 epoch = 0;
        if (arguments.size() >= 2 && input_rows_count != 0)
        {
            const auto & col_epoch = *arguments[1].column;
            epoch = col_epoch.getUInt(0);
        }

        auto col_res = ColumnDateTime::create(input_rows_count);
        auto & res_data = col_res->getData();

        if (const auto * col_src_non_const = typeid_cast<const ColumnUInt64 *>(&col_src))
        {
            const auto & src_data = col_src_non_const->getData();
            for (size_t i = 0; i < input_rows_count; ++i)
                res_data[i] = static_cast<UInt32>(((src_data[i] >> time_shift) + epoch) / 1000);
        }
        else if (const auto * col_src_const = typeid_cast<const ColumnConst *>(&col_src))
        {
            UInt64 src_val = col_src_const->getValue<UInt64>();
            for (size_t i = 0; i < input_rows_count; ++i)
                res_data[i] = static_cast<UInt32>(((src_val >> time_shift) + epoch) / 1000);
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal argument for function {}", name);

        return col_res;
    }
};


class FunctionSnowflakeIDToDateTime64 : public IFunction
{
private:
    const bool allow_nonconst_timezone_arguments;

public:
    static constexpr auto name = "snowflakeIDToDateTime64";

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionSnowflakeIDToDateTime64>(context); }
    explicit FunctionSnowflakeIDToDateTime64(ContextPtr context)
        : allow_nonconst_timezone_arguments(context->getSettingsRef()[Setting::allow_nonconst_timezone_arguments])
    {}

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"value", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isUInt64), nullptr, "UInt64"}
        };
        FunctionArgumentDescriptors optional_args{
            {"epoch", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNativeUInt), isColumnConst, "const UInt*"},
            {"time_zone", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"}
        };
        validateFunctionArguments(*this, arguments, args, optional_args);

        String timezone;
        if (arguments.size() == 3)
            timezone = extractTimeZoneNameFromFunctionArguments(arguments, 2, 0, allow_nonconst_timezone_arguments);

        return std::make_shared<DataTypeDateTime64>(3, timezone);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & col_src = *arguments[0].column;

        UInt64 epoch = 0;
        if (arguments.size() >= 2 && input_rows_count != 0)
        {
            const auto & col_epoch = *arguments[1].column;
            epoch = col_epoch.getUInt(0);
        }

        auto col_res = ColumnDateTime64::create(input_rows_count, 3);
        auto & res_data = col_res->getData();

        if (const auto * col_src_non_const = typeid_cast<const ColumnUInt64 *>(&col_src))
        {
            const auto & src_data = col_src_non_const->getData();
            for (size_t i = 0; i < input_rows_count; ++i)
                res_data[i] = (src_data[i] >> time_shift) + epoch;
        }
        else if (const auto * col_src_const = typeid_cast<const ColumnConst *>(&col_src))
        {
            UInt64 src_val = col_src_const->getValue<UInt64>();
            for (size_t i = 0; i < input_rows_count; ++i)
                res_data[i] = (src_val >> time_shift) + epoch;
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal argument for function {}", name);

        return col_res;

    }
};

REGISTER_FUNCTION(SnowflakeIDToDateTime)
{
    /// snowflakeIDToDateTime documentation
    FunctionDocumentation::Description description_snowflakeIDToDateTime = R"(
Returns the timestamp component of a [Snowflake ID](https://en.wikipedia.org/wiki/Snowflake_ID) as a value of type [DateTime](../data-types/datetime.md).
    )";
    FunctionDocumentation::Syntax syntax_snowflakeIDToDateTime = "snowflakeIDToDateTime(value[, epoch[, time_zone]])";
    FunctionDocumentation::Arguments arguments_snowflakeIDToDateTime = {
        {"value", "Snowflake ID.", {"UInt64"}},
        {"epoch", "Optional. Epoch of the Snowflake ID in milliseconds since 1970-01-01. Defaults to 0 (1970-01-01). For the Twitter/X epoch (2015-01-01), provide 1288834974657.", {"UInt*"}},
        {"time_zone", "Optional. [Timezone](/operations/server-configuration-parameters/settings.md#timezone). The function parses `time_string` according to the timezone.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_snowflakeIDToDateTime = {"Returns the timestamp component of `value`.", {"DateTime"}};
    FunctionDocumentation::Examples examples_snowflakeIDToDateTime = {
    {
        "Usage example",
        R"(
SELECT snowflakeIDToDateTime(7204436857747984384) AS res
        )",
        R"(
┌─────────────────res─┐
│ 2024-06-06 10:59:58 │
└─────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_snowflakeIDToDateTime = {24, 6};
    FunctionDocumentation::Category category_snowflakeIDToDateTime = FunctionDocumentation::Category::UUID;
    FunctionDocumentation documentation_snowflakeIDToDateTime = {description_snowflakeIDToDateTime, syntax_snowflakeIDToDateTime, arguments_snowflakeIDToDateTime, returned_value_snowflakeIDToDateTime, examples_snowflakeIDToDateTime, introduced_in_snowflakeIDToDateTime, category_snowflakeIDToDateTime};

    factory.registerFunction<FunctionSnowflakeIDToDateTime>(documentation_snowflakeIDToDateTime);

    /// snowflakeIDToDateTime64 documentation
    FunctionDocumentation::Description description_snowflakeIDToDateTime64 = R"(
Returns the timestamp component of a [Snowflake ID](https://en.wikipedia.org/wiki/Snowflake_ID) as a value of type [DateTime64](../data-types/datetime64.md).
    )";
    FunctionDocumentation::Syntax syntax_snowflakeIDToDateTime64 = "snowflakeIDToDateTime64(value[, epoch[, time_zone]])";
    FunctionDocumentation::Arguments arguments_snowflakeIDToDateTime64 = {
        {"value", "Snowflake ID.", {"UInt64"}},
        {"epoch", "Optional. Epoch of the Snowflake ID in milliseconds since 1970-01-01. Defaults to 0 (1970-01-01). For the Twitter/X epoch (2015-01-01), provide 1288834974657.", {"UInt*"}},
        {"time_zone", "Optional. [Timezone](/operations/server-configuration-parameters/settings.md#timezone). The function parses `time_string` according to the timezone.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_snowflakeIDToDateTime64 = {"Returns the timestamp component of `value` as a `DateTime64` with scale = 3, i.e. millisecond precision.", {"DateTime64"}};
    FunctionDocumentation::Examples examples_snowflakeIDToDateTime64 = {
    {
        "Usage example",
        R"(
SELECT snowflakeIDToDateTime64(7204436857747984384) AS res
        )",
        R"(
┌─────────────────res─┐
│ 2024-06-06 10:59:58 │
└─────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_snowflakeIDToDateTime64 = {24, 6};
    FunctionDocumentation::Category category_snowflakeIDToDateTime64 = FunctionDocumentation::Category::UUID;
    FunctionDocumentation documentation_snowflakeIDToDateTime64 = {description_snowflakeIDToDateTime64, syntax_snowflakeIDToDateTime64, arguments_snowflakeIDToDateTime64, returned_value_snowflakeIDToDateTime64, examples_snowflakeIDToDateTime64, introduced_in_snowflakeIDToDateTime64, category_snowflakeIDToDateTime64};

    factory.registerFunction<FunctionSnowflakeIDToDateTime64>(documentation_snowflakeIDToDateTime64);
}

}
