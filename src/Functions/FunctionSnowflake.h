#pragma once

#include <Functions/extractTimeZoneFromFunctionArguments.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>

#include <common/arithmeticOverflow.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


class FunctionDateTimeToSnowflake : public IFunction
{
private:
    const char * name;
public:
    FunctionDateTimeToSnowflake(const char * name_)
        : name(name_)
    {
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isVariadic() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!isDateTime(arguments[0].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The only argument for function {} must be DateTime", name);

        return std::make_shared<DataTypeInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & src = arguments[0];
        const auto & col = *src.column;

        auto res_column = ColumnInt64::create(input_rows_count);
        auto & result_data = res_column->getData();

        const auto & source_data = typeid_cast<const ColumnUInt32 &>(col).getData();
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            result_data[i] = (int64_t(source_data[i])*1000-1288834974657)<<22;
        }

        return res_column;
    }
};


class FunctionSnowflakeToDateTime : public IFunction
{
private:
    const char * name;
public:
    FunctionSnowflakeToDateTime(const char * name_)
        : name(name_)
    {
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() < 1 || arguments.size() > 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} takes one or two arguments", name);

        if (!typeid_cast<const DataTypeInt64 *>(arguments[0].type.get()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The first argument for function {} must be Int64", name);

        std::string timezone;
        if (arguments.size() == 2)
            timezone = extractTimeZoneNameFromFunctionArguments(arguments, 1, 0);

        return std::make_shared<DataTypeDateTime>(timezone);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & src = arguments[0];
        const auto & col = *src.column;

        auto res_column = ColumnUInt32::create(input_rows_count);
        auto & result_data = res_column->getData();

        const auto & source_data = typeid_cast<const ColumnInt64 &>(col).getData();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            result_data[i] = ((source_data[i]>>22)+1288834974657)/1000;
        }

        return res_column;
    }
};


class FunctionDateTime64ToSnowflake : public IFunction
{
private:
    const char * name;
public:
    FunctionDateTime64ToSnowflake(const char * name_)
        : name(name_)
    {
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isVariadic() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!isDateTime64(arguments[0].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The only argument for function {} must be DateTime64", name);

        return std::make_shared<DataTypeInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & src = arguments[0];
        const auto & col = *src.column;

        auto res_column = ColumnInt64::create(input_rows_count);
        auto & result_data = res_column->getData();

        const auto & source_data = typeid_cast<const ColumnDecimal<DateTime64> &>(col).getData();
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            result_data[i] = (source_data[i]-1288834974657)<<22;
        }

        return res_column;
    }
};


class FunctionSnowflakeToDateTime64 : public IFunction
{
private:
    const char * name;
public:
    FunctionSnowflakeToDateTime64(const char * name_)
        : name(name_)
    {
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {

        if (arguments.size() < 1 || arguments.size() > 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} takes one or two arguments", name);

        if (!typeid_cast<const DataTypeInt64 *>(arguments[0].type.get()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The first argument for function {} must be Int64", name);

        std::string timezone;
        if (arguments.size() == 2)
            timezone = extractTimeZoneNameFromFunctionArguments(arguments, 1, 0);

        return std::make_shared<DataTypeDateTime64>(3, timezone);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & src = arguments[0];
        const auto & col = *src.column;

        auto res_column = ColumnDecimal<DateTime64>::create(input_rows_count, 3);
        auto & result_data = res_column->getData();

        const auto & source_data = typeid_cast<const ColumnInt64 &>(col).getData();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            result_data[i] = (source_data[i]>>22)+1288834974657;
        }

        return res_column;
    }
};

}
