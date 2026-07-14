#pragma once

#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

/// arrayConcat(arr1, ...) - concatenate arrays.
class FunctionArrayConcat : public IFunction
{
public:
    static constexpr auto name = "arrayConcat";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayConcat>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;

    bool useDefaultImplementationForConstants() const override { return true; }
};

}
