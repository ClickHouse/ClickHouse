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
    {
        FunctionDocumentation::Description description = R"(Converts a [DateTime](../data-types/datetime.md) value to the first [Snowflake ID](https://en.wikipedia.org/wiki/Snowflake_ID) at the giving time.)";
        FunctionDocumentation::Syntax syntax = "dateTimeToSnowflakeID(value[, epoch])";
        FunctionDocumentation::Arguments arguments = {
            {"value", "Date with time. [DateTime](../data-types/datetime.md)."},
            {"epoch", "Epoch of the Snowflake ID in milliseconds since 1970-01-01. Defaults to 0 (1970-01-01). For the Twitter/X epoch (2015-01-01), provide 1288834974657. Optional. [UInt*](../data-types/int-uint.md)"}
        };
        FunctionDocumentation::ReturnedValue returned_value = "Input value converted to [UInt64](../data-types/int-uint.md) as the first Snowflake ID at that time.";
        FunctionDocumentation::Examples examples = {{"simple", "SELECT dateTimeToSnowflakeID(toDateTime('2021-08-15 18:57:56', 'Asia/Shanghai'))", "6832626392367104000"}};
        FunctionDocumentation::Categories categories = {"Snowflake ID"};

        factory.registerFunction<FunctionDateTimeToSnowflakeID>({description, syntax, arguments, returned_value, examples, categories});
    }

    {
        FunctionDocumentation::Description description = R"(Converts a [DateTime64](../data-types/datetime64.md) value to the first [Snowflake ID](https://en.wikipedia.org/wiki/Snowflake_ID) at the giving time.)";
        FunctionDocumentation::Syntax syntax = "dateTime64ToSnowflakeID(value[, epoch])";
        FunctionDocumentation::Arguments arguments = {
            {"value", "Date with time. [DateTime64](../data-types/datetime.md)."},
            {"epoch", "Epoch of the Snowflake ID in milliseconds since 1970-01-01. Defaults to 0 (1970-01-01). For the Twitter/X epoch (2015-01-01), provide 1288834974657. Optional. [UInt*](../data-types/int-uint.md)"}
        };
        FunctionDocumentation::ReturnedValue returned_value = "Input value converted to [UInt64](../data-types/int-uint.md) as the first Snowflake ID at that time.";
        FunctionDocumentation::Examples examples = {{"simple", "SELECT dateTime64ToSnowflakeID(toDateTime64('2021-08-15 18:57:56', 3, 'Asia/Shanghai'))", "6832626394434895872"}};
        FunctionDocumentation::Categories categories = {"Snowflake ID"};

        factory.registerFunction<FunctionDateTime64ToSnowflakeID>({description, syntax, arguments, returned_value, examples, categories});
    }
}

}
