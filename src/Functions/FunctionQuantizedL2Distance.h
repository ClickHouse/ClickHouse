
#pragma once

#include <Columns/ColumnConst.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include "FunctionDequantize16Bit.h"

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int ILLEGAL_COLUMN;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

extern FunctionPtr createFunctionArrayL2Distance(ContextPtr context_);

class FunctionQuantizedL2Distance : public IFunction
{
public:
    static constexpr auto name = "quantizedL2Distance";
    static FunctionPtr create(ContextPtr context)
    {
        dequantize_function = FunctionDequantize16Bit::create(context);
        l2_distance_function = createFunctionArrayL2Distance(context);
        return std::make_shared<FunctionQuantizedL2Distance>();
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeFloat32>(); }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (arguments.size() != 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} expects 2 arguments, got {}", getName(), arguments.size());

        ColumnsWithTypeAndName dequant_args_a = {{arguments[0].column, arguments[0].type, arguments[0].name}};
        ColumnsWithTypeAndName dequant_args_b = {{arguments[1].column, arguments[1].type, arguments[1].name}};

        ColumnPtr dequantized_a = dequantize_function->executeImpl(
            dequant_args_a, dequantize_function->getReturnTypeImpl({arguments[0].type}), input_rows_count);

        ColumnPtr dequantized_b = dequantize_function->executeImpl(
            dequant_args_b, dequantize_function->getReturnTypeImpl({arguments[1].type}), input_rows_count);

        auto array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>());

        ColumnsWithTypeAndName distance_args = {{dequantized_a, array_type, ""}, {dequantized_b, array_type, ""}};

        return l2_distance_function->executeImpl(distance_args, result_type, input_rows_count);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

private:
    static FunctionPtr dequantize_function;
    static FunctionPtr l2_distance_function;
};

}
