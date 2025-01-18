#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnNumbers.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>

namespace DB
{

class FunctionDequantize16Bit : public IFunction
{
public:
    static constexpr auto name = "dequantize16Bit";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionDequantize16Bit>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const ColumnsWithTypeAndName &) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;
    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    static void dequantize16Bit(const UInt8 * input, float * output, size_t size);

private:
    static void processArray(
        const PaddedPODArray<UInt8> & src_data,
        size_t array_size,
        PaddedPODArray<Float32> & result_data,
        size_t row,
        size_t fixed_string_length);
};

}
