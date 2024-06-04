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
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
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
    const char * name;

public:
    explicit FunctionDateTimeToSnowflake(const char * name_) : name(name_) { }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"value", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isDateTime), nullptr, "DateTime"}
        };
        validateFunctionArgumentTypes(*this, arguments, args);

        return std::make_shared<DataTypeInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
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
    const char * name;
    const bool allow_nonconst_timezone_arguments;

public:
    explicit FunctionSnowflakeToDateTime(const char * name_, ContextPtr context)
        : name(name_)
        , allow_nonconst_timezone_arguments(context->getSettings().allow_nonconst_timezone_arguments)
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
        validateFunctionArgumentTypes(*this, arguments, mandatory_args, optional_args);

        String timezone;
        if (arguments.size() == 2)
            timezone = extractTimeZoneNameFromFunctionArguments(arguments, 1, 0, allow_nonconst_timezone_arguments);

        return std::make_shared<DataTypeDateTime>(timezone);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
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
    const char * name;

public:
    explicit FunctionDateTime64ToSnowflake(const char * name_) : name(name_) { }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"value", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isDateTime64), nullptr, "DateTime64"}
        };
        validateFunctionArgumentTypes(*this, arguments, args);

        return std::make_shared<DataTypeInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
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
    const char * name;
    const bool allow_nonconst_timezone_arguments;

public:
    explicit FunctionSnowflakeToDateTime64(const char * name_, ContextPtr context)
        : name(name_)
        , allow_nonconst_timezone_arguments(context->getSettings().allow_nonconst_timezone_arguments)
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
        validateFunctionArgumentTypes(*this, arguments, mandatory_args, optional_args);

        String timezone;
        if (arguments.size() == 2)
            timezone = extractTimeZoneNameFromFunctionArguments(arguments, 1, 0, allow_nonconst_timezone_arguments);

        return std::make_shared<DataTypeDateTime64>(3, timezone);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
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

REGISTER_FUNCTION(DateTimeToSnowflake)
{
    factory.registerFunction("dateTimeToSnowflake",
        [](ContextPtr){ return std::make_shared<FunctionDateTimeToSnowflake>("dateTimeToSnowflake"); });
}

REGISTER_FUNCTION(DateTime64ToSnowflake)
{
    factory.registerFunction("dateTime64ToSnowflake",
        [](ContextPtr){ return std::make_shared<FunctionDateTime64ToSnowflake>("dateTime64ToSnowflake"); });
}

REGISTER_FUNCTION(SnowflakeToDateTime)
{
    factory.registerFunction("snowflakeToDateTime",
        [](ContextPtr context){ return std::make_shared<FunctionSnowflakeToDateTime>("snowflakeToDateTime", context); });
}
REGISTER_FUNCTION(SnowflakeToDateTime64)
{
    factory.registerFunction("snowflakeToDateTime64",
        [](ContextPtr context){ return std::make_shared<FunctionSnowflakeToDateTime64>("snowflakeToDateTime64", context); });
}

}
