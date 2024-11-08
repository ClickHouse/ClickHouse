#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>
#include <DataTypes/DataTypeFixedString.h>

namespace DB
{

class FunctionDequantize : public IFunction
{
public:
    static constexpr auto name = "dequantize";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionDequantize>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;
};

Float32 dequantizeFromBits(UInt32 quantized_value, UInt8 bit_width);
Float32 uint16ToFloat32(UInt16 h);

}
