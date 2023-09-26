#pragma once

#include <Functions/extractTimeZoneFromFunctionArguments.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/Context.h>

#include <base/arithmeticOverflow.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/** According to Twitter's post on Snowflake, we can extract the timestamp for a snowflake ID by right shifting
 * the snowflake ID by 22 bits(10 bits machine ID and 12 bits sequence ID) and adding the Twitter epoch time of 1288834974657.
 * https://en.wikipedia.org/wiki/Snowflake_ID
 * https://blog.twitter.com/engineering/en_us/a/2010/announcing-snowflake
 * https://ws-dl.blogspot.com/2019/08/2019-08-03-tweetedat-finding-tweet.html
*/
static constexpr size_t snowflake_epoch = 1288834974657L;
static constexpr int time_shift = 22;

class FunctionDateTimeToSnowflake : public IFunction
{
private:
    const char * name;

public:
    explicit FunctionDateTimeToSnowflake(const char * name_) : name(name_) { }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isVariadic() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!isDateTime(arguments[0].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The only argument for function {} must be DateTime", name);

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
        if (arguments.empty() || arguments.size() > 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} takes one or two arguments", name);

        if (!typeid_cast<const DataTypeInt64 *>(arguments[0].type.get()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The first argument for function {} must be Int64", name);

        std::string timezone;
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
    bool isVariadic() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!isDateTime64(arguments[0].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The only argument for function {} must be DateTime64", name);

        return std::make_shared<DataTypeInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & src = arguments[0];
        const auto & src_column = *src.column;

        auto res_column = ColumnInt64::create(input_rows_count);
        auto & res_data = res_column->getData();

        const auto & src_data = typeid_cast<const ColumnDecimal<DateTime64> &>(src_column).getData();
        for (size_t i = 0; i < input_rows_count; ++i)
            res_data[i] = (src_data[i] - snowflake_epoch) << time_shift;

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
        if (arguments.empty() || arguments.size() > 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} takes one or two arguments", name);

        if (!typeid_cast<const DataTypeInt64 *>(arguments[0].type.get()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The first argument for function {} must be Int64", name);

        std::string timezone;
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
