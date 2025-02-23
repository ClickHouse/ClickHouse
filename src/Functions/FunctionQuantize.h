#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnNumbers.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/PerformanceAdaptors.h>
#include <IO/WriteHelpers.h>
#include <Common/TargetSpecific.h>
#include "Functions/FunctionHelpers.h"

namespace DB
{

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_COLUMN;
}

template <typename Traits, typename QuantizeImpl>
class FunctionQuantizeBase : public IFunction
{
public:
    static constexpr auto name = Traits::name;

    String getName() const override { return name; }

    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionQuantizeBase>(); }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, expected 1",
                getName(),
                arguments.size());

        const DataTypeArray * array_type = typeid_cast<const DataTypeArray *>(arguments[0].get());
        if (!array_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument of function {} must be an array", getName());

        const auto * nested_type = array_type->getNestedType().get();
        if (!isFloat(nested_type->getTypeId()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Array elements must be Float32 or Float64");

        return std::make_shared<DataTypeFixedString>(1);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnArray * col_array = nullptr;
        if (const auto * col_const = checkAndGetColumnConst<ColumnArray>(arguments[0].column.get()))
            col_array = checkAndGetColumn<ColumnArray>(col_const->getDataColumnPtr().get());
        else
            col_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());

        if (!col_array)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument of function {} must be an array", getName());

        const auto & offsets = col_array->getOffsets();
        if (offsets.empty())
            return ColumnFixedString::create(1);

        size_t array_size = offsets[0];
        for (size_t i = 1; i < offsets.size(); ++i)
        {
            if (offsets[i] - offsets[i - 1] != array_size)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "All arrays must have the same size");
        }

        size_t fixed_string_length = array_size * Traits::multiplier;
        auto result_column = ColumnFixedString::create(fixed_string_length);
        auto & result_chars = result_column->getChars();
        result_chars.resize_fill(input_rows_count * fixed_string_length);

        if (array_size == 0)
        {
            return result_column;
        }

        const auto & array_data = col_array->getData();
        if (const auto * col_float32 = checkAndGetColumn<ColumnFloat32>(&array_data))
        {
            size_t offset = 0;
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                QuantizeImpl::execute(
                    col_float32->getData().data() + offset, result_chars.data() + i * fixed_string_length, fixed_string_length);
                offset += array_size;
            }
        }
        else if (const auto * col_float64 = checkAndGetColumn<ColumnFloat64>(&array_data))
        {
            size_t offset = 0;
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                QuantizeImpl::execute(
                    col_float64->getData().data() + offset, result_chars.data() + i * fixed_string_length, fixed_string_length);
                offset += array_size;
            }
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected array element type");

        return result_column;
    }
};

} // namespace DB
