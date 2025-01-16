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

class FunctionQuantize : public IFunction
{
public:
    static constexpr auto name = "quantize";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionQuantize>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool isInjective(const ColumnsWithTypeAndName &) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;
    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;


    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

private:
    static UInt32 quantizeToBits(Float64 value, UInt8 bit_width);
    static UInt16 float32ToUInt16(Float32 value);

    template <typename T>
    static void processArray(
        const PaddedPODArray<T> & src_data,
        size_t array_size,
        UInt8 bit_width,
        PaddedPODArray<UInt8> & result_chars,
        size_t row,
        size_t fixed_string_length);
};

}
