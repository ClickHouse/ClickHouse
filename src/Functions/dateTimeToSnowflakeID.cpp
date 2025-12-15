#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <Core/DecimalFunctions.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace
{

/// See generateSnowflakeID.cpp
constexpr size_t time_shift = 22;

}

class FunctionDateTimeToSnowflakeID : public IFunction
{
public:
    static constexpr auto name = "dateTimeToSnowflakeID";

    static FunctionPtr create(ContextPtr /*context*/) { return std::make_shared<FunctionDateTimeToSnowflakeID>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"value", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isDateTime), nullptr, "DateTime"}
        };
        FunctionArgumentDescriptors optional_args{
            {"epoch", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNativeUInt), isColumnConst, "const UInt*"}
        };
        validateFunctionArguments(*this, arguments, args, optional_args);

        return std::make_shared<DataTypeUInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & col_src = *arguments[0].column;

        UInt64 epoch = 0;
        if (arguments.size() == 2 && input_rows_count != 0)
        {
            const auto & col_epoch = *arguments[1].column;
            epoch = col_epoch.getUInt(0);
        }

        auto col_res = ColumnUInt64::create(input_rows_count);
        auto & res_data = col_res->getData();

        const auto & src_data = typeid_cast<const ColumnDateTime &>(col_src).getData();
        for (size_t i = 0; i < input_rows_count; ++i)
            res_data[i] = (static_cast<UInt64>(src_data[i]) * 1000 - epoch) << time_shift;
        return col_res;
    }
};


class FunctionDateTime64ToSnowflakeID : public IFunction
{
public:
    static constexpr auto name = "dateTime64ToSnowflakeID";

    static FunctionPtr create(ContextPtr /*context*/) { return std::make_shared<FunctionDateTime64ToSnowflakeID>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"value", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isDateTime64), nullptr, "DateTime64"}
        };
        FunctionArgumentDescriptors optional_args{
            {"epoch", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNativeUInt), isColumnConst, "const UInt*"}
        };
        validateFunctionArguments(*this, arguments, args, optional_args);

        return std::make_shared<DataTypeUInt64>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & col_src = *arguments[0].column;
        const auto & src_data = typeid_cast<const ColumnDateTime64 &>(col_src).getData();

        UInt64 epoch = 0;
        if (arguments.size() == 2 && input_rows_count != 0)
        {
            const auto & col_epoch = *arguments[1].column;
            epoch = col_epoch.getUInt(0);
        }

        auto col_res = ColumnUInt64::create(input_rows_count);
        auto & res_data = col_res->getData();

        /// timestamps in snowflake-ids are millisecond-based, convert input to milliseconds
        UInt32 src_scale = getDecimalScale(*arguments[0].type);
        Int64 multiplier_msec = DecimalUtils::scaleMultiplier<DateTime64>(3);
        Int64 multiplier_src = DecimalUtils::scaleMultiplier<DateTime64>(src_scale);
        auto factor = multiplier_msec / static_cast<double>(multiplier_src);

        for (size_t i = 0; i < input_rows_count; ++i)
            res_data[i] = std::llround(src_data[i] * factor - epoch) << time_shift;

        return col_res;
    }
};

REGISTER_FUNCTION(DateTimeToSnowflakeID)
{
    /// dateTimeToSnowflakeID documentation
    FunctionDocumentation::Description description_dateTimeToSnowflakeID = R"(
Converts a [DateTime](../data-types/datetime.md) value to the first [Snowflake ID](https://en.wikipedia.org/wiki/Snowflake_ID) at the giving time.
    )";
    FunctionDocumentation::Syntax syntax_dateTimeToSnowflakeID = "dateTimeToSnowflakeID(value[, epoch])";
    FunctionDocumentation::Arguments arguments_dateTimeToSnowflakeID = {
        {"value", "Date with time.", {"DateTime", "DateTime64"}},
        {"epoch", "Optional. Epoch of the Snowflake ID in milliseconds since 1970-01-01. Defaults to 0 (1970-01-01). For the Twitter/X epoch (2015-01-01), provide 1288834974657.", {"UInt*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_dateTimeToSnowflakeID = {"Returns the input value as the first Snowflake ID at that time.", {"UInt64"}};
    FunctionDocumentation::Examples examples_dateTimeToSnowflakeID = {
    {
        "Usage example",
        R"(
SELECT toDateTime('2021-08-15 18:57:56', 'Asia/Shanghai') AS dt, dateTimeToSnowflakeID(dt) AS res;
        )",
        R"(
┌──────────────────dt─┬─────────────────res─┐
│ 2021-08-15 18:57:56 │ 6832626392367104000 │
└─────────────────────┴─────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_dateTimeToSnowflakeID = {24, 6};
    FunctionDocumentation::Category category_dateTimeToSnowflakeID = FunctionDocumentation::Category::UUID;
    FunctionDocumentation documentation_dateTimeToSnowflakeID = {description_dateTimeToSnowflakeID, syntax_dateTimeToSnowflakeID, arguments_dateTimeToSnowflakeID, returned_value_dateTimeToSnowflakeID, examples_dateTimeToSnowflakeID, introduced_in_dateTimeToSnowflakeID, category_dateTimeToSnowflakeID};

    factory.registerFunction<FunctionDateTimeToSnowflakeID>(documentation_dateTimeToSnowflakeID);

    /// dateTime64ToSnowflakeID documentation
    FunctionDocumentation::Description description_dateTime64ToSnowflakeID = R"(
Converts a [`DateTime64`](../data-types/datetime64.md) to the first [Snowflake ID](https://en.wikipedia.org/wiki/Snowflake_ID) at the giving time.

See section ["Snowflake ID generation"](#snowflake-id-generation) for implementation details.
    )";
    FunctionDocumentation::Syntax syntax_dateTime64ToSnowflakeID = "dateTime64ToSnowflakeID(value[, epoch])";
    FunctionDocumentation::Arguments arguments_dateTime64ToSnowflakeID = {
        {"value", "Date with time.", {"DateTime", "DateTime64"}},
        {"epoch", "Epoch of the Snowflake ID in milliseconds since 1970-01-01. Defaults to 0 (1970-01-01). For the Twitter/X epoch (2015-01-01), provide 1288834974657.", {"UInt*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_dateTime64ToSnowflakeID = {"Returns the input value as the first Snowflake ID at that time.", {"UInt64"}};
    FunctionDocumentation::Examples examples_dateTime64ToSnowflakeID = {
   {
       "Usage example",
       R"(
SELECT toDateTime64('2025-08-15 18:57:56.493', 3, 'Asia/Shanghai') AS dt, dateTime64ToSnowflakeID(dt) AS res;
       )",
       R"(
┌──────────────────────dt─┬─────────────────res─┐
│ 2025-08-15 18:57:56.493 │ 7362075066076495872 │
└─────────────────────────┴─────────────────────┘
       )"
       }
    };
    FunctionDocumentation::IntroducedIn introduced_in_dateTime64ToSnowflakeID = {24, 6};
    FunctionDocumentation::Category category_dateTime64ToSnowflakeID = FunctionDocumentation::Category::UUID;
    FunctionDocumentation documentation_dateTime64ToSnowflakeID = {description_dateTime64ToSnowflakeID, syntax_dateTime64ToSnowflakeID, arguments_dateTime64ToSnowflakeID, returned_value_dateTime64ToSnowflakeID, examples_dateTime64ToSnowflakeID, introduced_in_dateTime64ToSnowflakeID, category_dateTime64ToSnowflakeID};

    factory.registerFunction<FunctionDateTime64ToSnowflakeID>(documentation_dateTime64ToSnowflakeID);
}

}
