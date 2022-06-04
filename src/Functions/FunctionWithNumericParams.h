#pragma once

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/castColumn.h>

#include <Common/DateLUT.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/// Common logic to handle numeric arguments like year, month, day, hour, minute, second
class FunctionWithNumericParamsBase : public IFunction
{
public:
    bool isInjective(const ColumnsWithTypeAndName &) const override
    {
        return false; /// invalid argument values and timestamps that are out of supported range are converted into a default value
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForNulls() const override { return true; }

    bool useDefaultImplementationForConstants() const override { return true; }

protected:
    template <class AgrumentNames>
    void checkRequiredArguments(const ColumnsWithTypeAndName & arguments, const AgrumentNames & argument_names, const size_t optional_argument_count) const
    {
        if (arguments.size() < argument_names.size() || arguments.size() > argument_names.size() + optional_argument_count)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Function {} requires {} to {} arguments, but {} given",
                            getName(), argument_names.size(), argument_names.size() + optional_argument_count, arguments.size());

        for (size_t i = 0; i < argument_names.size(); ++i)
        {
            DataTypePtr argument_type = arguments[i].type;
            if (!isNumber(argument_type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "Argument '{}' for function {} must be number", std::string(argument_names[i]), getName());
        }
    }

    template <class AgrumentNames>
    void convertRequiredArguments(const ColumnsWithTypeAndName & arguments, const AgrumentNames & argument_names, Columns & converted_arguments, const DataTypePtr converted_argument_type) const
    {
        converted_arguments.clear();
        converted_arguments.reserve(arguments.size());
        for (size_t i = 0; i < argument_names.size(); ++i)
        {
            ColumnPtr argument_column = castColumn(arguments[i], converted_argument_type);
            argument_column = argument_column->convertToFullColumnIfConst();
            converted_arguments.push_back(argument_column);
        }
    }
};

}
