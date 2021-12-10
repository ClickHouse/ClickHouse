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
    extern const int DECIMAL_OVERFLOW;
}

/// Cast DateTime64 to Int64 representation narrowed down (or scaled up) to any scale value defined in Impl.
class FunctionToUnixTimestamp64 : public IFunction
{
private:
    size_t target_scale;
    const char * name;
public:
    FunctionToUnixTimestamp64(size_t target_scale_, const char * name_)
        : target_scale(target_scale_), name(name_)
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

        Int32 scale_diff = typeid_cast<const DataTypeDateTime64 &>(*src.type).getScale() - target_scale;
        if (scale_diff == 0)
        {
            for (size_t i = 0; i < input_rows_count; ++i)
                result_data[i] = source_data[i];
        }
        else if (scale_diff < 0)
        {
            const Int64 scale_multiplier = DecimalUtils::scaleMultiplier<Int64>(-scale_diff);
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                Int64 result_value = source_data[i];
                if (common::mulOverflow(result_value, scale_multiplier, result_value))
                    throw Exception("Decimal overflow in " + getName(), ErrorCodes::DECIMAL_OVERFLOW);

                result_data[i] = result_value;
            }
        }
        else
        {
            const Int64 scale_multiplier = DecimalUtils::scaleMultiplier<Int64>(scale_diff);
            for (size_t i = 0; i < input_rows_count; ++i)
                result_data[i] = Int64(source_data[i]) / scale_multiplier;
        }

        return res_column;
    }
};


class FunctionFromUnixTimestamp64 : public IFunction
{
private:
    size_t target_scale;
    const char * name;
public:
    FunctionFromUnixTimestamp64(size_t target_scale_, const char * name_)
        : target_scale(target_scale_), name(name_)
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

        return std::make_shared<DataTypeDateTime64>(target_scale, timezone);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & src = arguments[0];
        const auto & col = *src.column;

        auto res_column = ColumnDecimal<DateTime64>::create(input_rows_count, target_scale);
        auto & result_data = res_column->getData();

        const auto & source_data = typeid_cast<const ColumnInt64 &>(col).getData();

        for (size_t i = 0; i < input_rows_count; ++i)
            result_data[i] = source_data[i];

        return res_column;
    }
};

}
